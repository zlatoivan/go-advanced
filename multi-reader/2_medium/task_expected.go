package main

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// SizedReadCloser - интерфейс ридера с возможностью чтения/закрытия и знанием своего размера.
type SizedReadCloser interface {
	io.ReadCloser
	Size() int64
}

// MultiReader объединяет несколько SizedReadCloser в единый конкатенированный поток и поддерживает асинхронный префетч
type MultiReader struct {
	readers    []SizedReadCloser  // исходные ридеры
	totalSize  int64              // Суммарный размер всех внутренних ридеров
	bufferSize int64              // размер одного блока префетча
	buffersNum int                // количество буферов
	mu         sync.Mutex         // мьютекс для блокировок, блокирует все нижние поля:
	windowBuf  []byte             // текущее окно данных
	pfBufCh    chan []byte        // буферизированный канал блоков, наполняется префетчером
	pfErrCh    chan error         // канал для ошибки/EOF от префетчера (ёмкость 1)
	pfCancel   context.CancelFunc // отмена контекста префетчера
	pfWg       sync.WaitGroup     // ожидание завершения горутины префетчера
	closed     bool               // флаг закрытия мультиридера
}

// Проверка, что MultiReader удовлетворяет интерфейсу SizedReadCloser
var _ SizedReadCloser = (*MultiReader)(nil)

// NewMultiReader создаёт конкатенированный ридер с поддержкой асинхронного префетча
func NewMultiReader(buffersSize int64, buffersNum int, readers ...SizedReadCloser) *MultiReader {
	var total int64
	for _, r := range readers {
		total += r.Size()
	}

	return &MultiReader{
		readers:    readers,
		totalSize:  total,
		buffersNum: buffersNum,
		bufferSize: buffersSize,
	}
}

// Read читает данные из внутреннего окна, пополняемого префетчером.
func (m *MultiReader) Read(p []byte) (n int, err error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if m.pfBufCh == nil { // Если префетч не начат, запускаем его
		m.pfBufCh = make(chan []byte, m.buffersNum)
		m.pfErrCh = make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		m.pfCancel = cancel
		m.pfWg.Add(1)
		go m.prefetchLoop(ctx)
	}
	m.mu.Unlock()

	for {
		m.mu.Lock()
		if len(m.windowBuf) != 0 { // Если данные в окне есть, то копируем их
			dst := p[n:]
			toCopy := min(len(dst), len(m.windowBuf))
			copy(dst[:toCopy], m.windowBuf[:toCopy])
			m.windowBuf = m.windowBuf[toCopy:]
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

// prefetchLoop - горутина префетча. Последовательно читает из всех ридеров и наполняет pfBufCh блоками.
// По завершении шлёт ошибку в pfErrCh.
func (m *MultiReader) prefetchLoop(ctx context.Context) {
	defer func() {
		close(m.pfBufCh)
		close(m.pfErrCh)
		m.pfWg.Done()
	}()

	for _, reader := range m.readers {
		for {
			buf := make([]byte, m.bufferSize)
			n, err := reader.Read(buf)
			if n > 0 {
				select {
				case <-ctx.Done():
					m.sendErr(ctx.Err())
					return
				case m.pfBufCh <- buf[:n]:
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				m.sendErr(err)
				return
			}
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
