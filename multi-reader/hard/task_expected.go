package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
)

// SizedReadSeekCloser - интерфейс ридера с возможностью seek и знанием своего размера.
type SizedReadSeekCloser interface {
	io.ReadSeekCloser
	Size() int64
}

// MultiReader объединяет несколько SizedReadSeekCloser в единый конкатенированный поток и поддерживает асинхронный префетч
type MultiReader struct {
	readers     []SizedReadSeekCloser // исходные ридеры
	totalSize   int64                 // суммарный размер всех источников
	prefixSizes []int64               // абсолютные стартовые позиции ридеров (префиксные суммы)
	absPos      int64                 // абсолютная позиция курсора чтения (пользователя)
	windowBuf   []byte                // текущее окно данных
	windowStart int64                 // абсолютная позиция начала окна
	bufferSize  int64                 // размер одного блока префетча
	buffersNum  int                   // количество буферов
	pfBufCh     chan []byte           // буферизированный канал блоков, наполняется префетчером
	pfErrCh     chan error            // канал для ошибки/EOF от префетчера (ёмкость 1)
	pfCancel    context.CancelFunc    // отмена контекста префетчера
	pfWg        sync.WaitGroup        // ожидание завершения горутины префетчера
	pfStarted   bool                  // флаг запуска префетчера
	mu          sync.Mutex            // мьютекс для блокировок
	closed      bool                  // флаг закрытия мультиридера
}

var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер с поддержкой асинхронного префетча
func NewMultiReader(buffersSize int64, buffersNum int, readers ...SizedReadSeekCloser) *MultiReader {
	prefixSizes := make([]int64, len(readers)+1)
	for i := 1; i < len(readers)+1; i++ {
		prefixSizes[i] = prefixSizes[i-1] + readers[i-1].Size()
	}

	return &MultiReader{
		readers:     readers,
		totalSize:   prefixSizes[len(readers)],
		prefixSizes: prefixSizes,
		buffersNum:  buffersNum,
		bufferSize:  buffersSize,
	}
}

// Read читает данные из внутреннего окна, пополняемого префетчером.
func (m *MultiReader) Read(p []byte) (n int, err error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if m.absPos == m.totalSize {
		m.mu.Unlock()
		return 0, io.EOF
	}
	if !m.pfStarted {
		m.startPrefetchLocked(m.absPos)
	}
	m.mu.Unlock()

	for n < len(p) {
		m.mu.Lock()
		if len(m.windowBuf) != 0 { // Если данные в окне есть
			dst := p[n:]
			toCopy := min(len(dst), len(m.windowBuf)) // Копируем и продвигаем курсоры
			copy(dst[:toCopy], m.windowBuf[:toCopy])
			m.windowBuf = m.windowBuf[toCopy:]
			m.windowStart += int64(toCopy)
			m.absPos += int64(toCopy)
			n += toCopy
			if n == len(p) {
				m.mu.Unlock()
				break
			}
			m.mu.Unlock()
			continue
		}
		m.mu.Unlock()

		buf, okPf := <-m.pfBufCh // Окно пусто - ждём новый блок от префетчера
		if !okPf {               // Канал данных закрыт - считываем итоговую ошибку/EOF
			select {
			case err = <-m.pfErrCh:
			default:
				err = io.EOF
			}
			return n, err
		}
		m.mu.Lock()
		m.windowBuf = append(m.windowBuf, buf...)
		m.mu.Unlock()
	}

	return n, nil
}

// Seek перемещает курсор
func (m *MultiReader) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, io.ErrClosedPipe
	}

	seekPos := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		seekPos += m.absPos
	case io.SeekEnd:
		seekPos += m.totalSize
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if seekPos < 0 || seekPos > m.totalSize {
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= totalSize (%d)", seekPos, m.totalSize)
	}

	delta := seekPos - m.windowStart
	switch {
	case 0 <= delta && delta < int64(len(m.windowBuf)): // Быстрый путь: позиция внутри текущего окна - только сдвигаем смещение
		m.windowBuf = m.windowBuf[delta:]
	default: // Вне окна: сбрасываем окно и перезапускаем префетч при следующем чтении
		m.windowBuf = nil
		if m.pfStarted {
			m.resetPrefetchLocked()
		}
	}

	m.windowStart = seekPos
	m.absPos = seekPos

	return seekPos, nil
}

// Close завершает префетч и закрывает все источники, агрегируя ошибки.
func (m *MultiReader) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	if m.pfCancel != nil {
		m.pfCancel()
	}
	m.mu.Unlock()

	m.pfWg.Wait()

	for _, r := range m.readers {
		err := r.Close()
		if err != nil {
			return fmt.Errorf("r.Close: %w", err)
		}
	}

	return nil
}

// Size возвращает суммарный размер всех ридеров.
func (m *MultiReader) Size() int64 {
	return m.totalSize
}

// startPrefetchLocked запускает горутину префетчера, читающую блоки в каналы.
func (m *MultiReader) startPrefetchLocked(startPos int64) {
	if m.pfStarted {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.pfBufCh = make(chan []byte, m.buffersNum)
	m.pfErrCh = make(chan error, 1)
	m.pfCancel = cancel
	m.pfStarted = true
	m.pfWg.Add(1)
	go m.prefetchLoop(ctx, startPos)
}

// prefetchLoop - горутина префетча. Наполняет pfBufCh блоками, по завершении шлёт ошибку в pfErrCh.
func (m *MultiReader) prefetchLoop(ctx context.Context, startPos int64) {
	defer func() {
		close(m.pfBufCh)
		close(m.pfErrCh)
		m.pfWg.Done()
	}()

	curPos := startPos
	curReaderIdx := -1

	for {
		// Общий EOF: больше данных не будет, уведомляем и завершаемся
		if curPos >= m.totalSize {
			sendErr(m.pfErrCh, io.EOF)
			return
		}

		// Выбор активного ридера и установка needSeek
		if curReaderIdx < 0 || !(m.prefixSizes[curReaderIdx] <= curPos && curPos < m.prefixSizes[curReaderIdx+1]) {
			curReaderIdx = sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
		}
		reader := m.readers[curReaderIdx]

		// Выполнение Seek и сброс needSeek
		localOffset := curPos - m.prefixSizes[curReaderIdx]
		_, err := reader.Seek(localOffset, io.SeekStart)
		if err != nil {
			sendErr(m.pfErrCh, err)
			return
		}

		// Выполнение Read
		nextReader := func() {
			curPos = m.prefixSizes[curReaderIdx+1]
			curReaderIdx = -1
		}
		remainInReader := m.prefixSizes[curReaderIdx+1] - curPos
		if remainInReader == 0 { // Достигли границы ридеров
			nextReader()
			continue
		}
		toRead := min(remainInReader, m.bufferSize)
		buf := make([]byte, toRead)
		n, err := reader.Read(buf)
		if n > 0 {
			select {
			case <-ctx.Done():
				sendErr(m.pfErrCh, ctx.Err())
				return
			case m.pfBufCh <- buf[:n]: // Ждем, пока окно освободиться, чтобы записать следующий блок
				curPos += int64(n) // Обновляем глобальную позицию на фактически прочитанные байты
			}
		}
		switch {
		case err == io.EOF:
			nextReader()
		case err != nil:
			sendErr(m.pfErrCh, err)
			return
		}
	}
}

// resetPrefetchLocked останавливает текущий префетч и сбрасывает его поля. Требует удержания m.mu
func (m *MultiReader) resetPrefetchLocked() {
	if m.pfCancel != nil {
		m.pfCancel()
	}
	m.pfWg.Wait() // Дождаться завершения старого префетчера, чтобы исключить параллельный доступ
	m.pfStarted = false
	m.pfBufCh = nil
	m.pfErrCh = nil
	m.pfCancel = nil
}

// sendErr отправляет ошибку в канал, если есть место
func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}
