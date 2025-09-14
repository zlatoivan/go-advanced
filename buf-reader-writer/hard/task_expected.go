package main

import (
	"context"
	"fmt"
	"io"
)

// MaxItems — максимальный размер объединённого батча для одного вызова Process.
const MaxItems = 9999

// Producer — источник данных. Возвращает элементы и cookie для последующего Commit.
type Producer interface {
	// Next returns:
	// - batch of items to be processed
	// - cookie to be commited when processing is done
	// - error
	Next() (items []any, cookie int, err error)
	// Commit is used to mark data batch as processed
	Commit(cookie int) error
}

// Consumer — потребитель данных. Обрабатывает переданные элементы.
type Consumer interface {
	Process(items []any) error
}

// batch — единица передачи в воркер: объединённые items из нескольких Next
// и упорядоченный набор cookies, которые требуется коммитить строго по порядку.
type batch struct {
	items   []any
	cookies []int
}

// startWorker поднимает горутину-воркер, которая:
// 1) вызывает Process для батча,
// 2) последовательно делает Commit для всех cookies,
// 3) отправляет ошибки в errCh и корректно завершается по ctx.Done() или закрытию batchCh.
func startWorker(ctx context.Context, p Producer, c Consumer) (chan batch, chan error, chan struct{}) {
	batchCh := make(chan batch, 1)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// Worker: последовательно Process, затем Commit всех cookies
	go func() {
		defer close(doneCh)
		for {
			select {
			case <-ctx.Done():
				return
			case b, ok := <-batchCh:
				if !ok {
					return
				}
				if len(b.items) == 0 {
					continue
				}
				var err error
				err = c.Process(b.items)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("push error: %w", err):
					default:
					}
					return
				}
				for _, ck := range b.cookies {
					err = p.Commit(ck)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("error commiting cookie %d: %w", ck, err):
						default:
						}
						return
					}
				}
			}
		}
	}()

	return batchCh, errCh, doneCh
}

// Pipe читает элементы из Producer, аккумулирует их до MaxItems и отправляет в воркер.
// Воркер выполняет Process и Commit по порядку. На io.EOF выполняется «флеш» хвоста
// и ожидание завершения воркера; при ошибках Next/Process/Commit — немедленный выход.
func Pipe(p Producer, c Consumer) error {
	var buf []any
	var cookies []int

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchCh, errCh, doneCh := startWorker(ctx, p, c)

	// flush отправляет текущий накопленный буфер в воркер и очищает локальные срезы.
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Canceled
		case batchCh <- batch{items: buf, cookies: cookies}:
		}
		// Сбросим локальный буфер
		buf = nil
		cookies = nil
		return nil
	}

	for {
		// Ранняя реакция на ошибку воркера, если она уже есть.
		select {
		case e := <-errCh:
			cancel()
			return e
		default:
		}

		items, cookie, err := p.Next()
		if err != nil {
			if err == io.EOF {
				// Источник завершился: флешим хвост, закрываем канал и ждём воркер.
				flushErr := flush()
				if flushErr != nil {
					cancel()
					return flushErr
				}
				close(batchCh)
				// Дождаться результата воркера: если он завершился ошибкой — вернуть её, иначе EOF
				select {
				case e := <-errCh:
					cancel()
					<-doneCh
					return e
				case <-doneCh:
					// На случай гонки проверим, не пришла ли ошибка
					select {
					case e := <-errCh:
						cancel()
						return e
					default:
					}
					return io.EOF
				}
			}
			cancel()
			return fmt.Errorf("read error: %w", err)
		}

		// Накопление: если не переполняем буфер — просто добавляем элементы и cookie.
		if len(buf)+len(items) <= MaxItems {
			buf = append(buf, items...)
			cookies = append(cookies, cookie)
			continue
		}

		// Переполнение: отправляем накопленное на обработку в воркер.
		err = flush()
		if err != nil {
			cancel()
			return err
		}

		// Начинаем новый буфер с текущего батча (эти items ещё не обрабатывались).
		buf = items
		cookies = []int{cookie}
	}
}
