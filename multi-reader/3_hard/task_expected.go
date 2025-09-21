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
	prefixSizes []int64               // абсолютные стартовые позиции ридеров (префиксные суммы)
	bufferSize  int64                 // размер одного блока префетча
	buffersNum  int                   // количество буферов
	mu          sync.Mutex            // мьютекс для блокировок, блокирует все нижние поля:
	windowBuf   []byte                // текущее окно данных
	windowStart int64                 // абсолютная позиция начала окна
	pfBufCh     chan []byte           // буферизированный канал блоков, наполняется префетчером
	pfErrCh     chan error            // канал для ошибки/EOF от префетчера (ёмкость 1)
	pfCancel    context.CancelFunc    // отмена контекста префетчера
	pfWg        sync.WaitGroup        // ожидание завершения горутины префетчера
	closed      bool                  // флаг закрытия мультиридера
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadSeekCloser
var _ SizedReadSeekCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер с поддержкой асинхронного префетча
func NewMultiReader(buffersSize int64, buffersNum int, readers ...SizedReadSeekCloser) *MultiReader {
	prefixSizes := make([]int64, len(readers)+1)
	for i := 1; i < len(readers)+1; i++ {
		prefixSizes[i] = prefixSizes[i-1] + readers[i-1].Size()
	}

	return &MultiReader{
		readers:     readers,
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
	if m.windowStart == m.Size() {
		m.mu.Unlock()
		return 0, io.EOF
	}
	if m.pfBufCh == nil { // Если префетч не начат, запускаем его
		m.pfBufCh = make(chan []byte, m.buffersNum)
		m.pfErrCh = make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		m.pfCancel = cancel
		m.pfWg.Add(1)
		go m.prefetchLoop(ctx, m.windowStart)
	}
	m.mu.Unlock()

	for {
		m.mu.Lock()
		if len(m.windowBuf) != 0 { // Если данные в окне есть, то копируем их и продвигаем курсоры
			dst := p[n:]
			toCopy := min(len(dst), len(m.windowBuf))
			copy(dst[:toCopy], m.windowBuf[:toCopy])
			m.windowBuf = m.windowBuf[toCopy:]
			m.windowStart += int64(toCopy)
			n += toCopy
			if n == len(p) {
				m.mu.Unlock()
				return n, nil
			}
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
		seekPos += m.windowStart
	case io.SeekEnd:
		seekPos += m.Size()
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if seekPos < 0 || seekPos > m.Size() {
		return 0, fmt.Errorf("seek position (%d) should be >= 0 and <= total size (%d)", seekPos, m.Size())
	}

	delta := seekPos - m.windowStart
	switch {
	case 0 <= delta && delta < int64(len(m.windowBuf)): // Быстрый путь: позиция внутри текущего окна - только сдвигаем смещение
		m.windowBuf = m.windowBuf[delta:]
	default: // Вне окна: сбрасываем окно и перезапускаем префетч при следующем чтении
		m.windowBuf = nil
		if m.pfCancel != nil {
			m.pfCancel()
		}
		m.pfWg.Wait()   // Дождаться завершения старого префетчера, чтобы исключить параллельный доступ
		m.pfBufCh = nil // Останавливаем текущий префетч и сбрасываем его поля
		m.pfErrCh = nil
		m.pfCancel = nil
	}

	m.windowStart = seekPos

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
	return m.prefixSizes[len(m.readers)]
}

// prefetchLoop - горутина префетча. Наполняет pfBufCh блоками, по завершении шлёт ошибку в pfErrCh.
func (m *MultiReader) prefetchLoop(ctx context.Context, startPos int64) {
	defer func() {
		close(m.pfBufCh)
		close(m.pfErrCh)
		m.pfWg.Done()
	}()

	curPos := startPos

	for curPos < m.Size() {
		curReaderIdx := sort.Search(len(m.readers), func(i int) bool { return m.prefixSizes[i+1] > curPos })
		reader := m.readers[curReaderIdx]

		_, err := reader.Seek(curPos-m.prefixSizes[curReaderIdx], io.SeekStart)
		if err != nil {
			m.sendErr(err)
			return
		}

		remainInReader := m.prefixSizes[curReaderIdx+1] - curPos
		if remainInReader == 0 { // Достигли границы ридеров
			curPos = m.prefixSizes[curReaderIdx+1]
			continue
		}
		toRead := min(remainInReader, m.bufferSize)
		buf := make([]byte, toRead)
		n, err := reader.Read(buf)
		if n > 0 {
			select {
			case <-ctx.Done():
				m.sendErr(ctx.Err())
				return
			case m.pfBufCh <- buf[:n]: // Ждем, пока окно освободиться, чтобы записать следующий блок
				curPos += int64(n) // Обновляем глобальную позицию на фактически прочитанные байты
			}
		}
		switch {
		case err == io.EOF:
			curPos = m.prefixSizes[curReaderIdx+1]
		case err != nil:
			m.sendErr(err)
			return
		}
	}

	m.sendErr(io.EOF)
}

// sendErr отправляет ошибку в канал, если есть место
func (m *MultiReader) sendErr(err error) {
	select {
	case m.pfErrCh <- err:
	default:
	}
}
