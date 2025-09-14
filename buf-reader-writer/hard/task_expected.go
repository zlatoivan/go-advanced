package main

import (
	"context"
	"fmt"
	"io"
	"sync"
)

const MaxItems = 9999

type Producer interface {
	// Next returns:
	// - batch of items to be processed
	// - cookie to be commited when processing is done
	// - error
	Next() (items []any, cookie int, err error)
	// Commit is used to mark data batch as processed
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

func Pipe(p Producer, c Consumer) error {
	var buf []any
	var cookies []int

	type commitReq struct {
		cookie int
		done   chan struct{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	readyCookies := make(chan commitReq, 256)

	// Горунита коммитов: сохраняет порядок и прекращает работу при ошибке
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-readyCookies:
				err := p.Commit(req.cookie)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("commit error: %w", err):
					default:
					}
					if req.done != nil {
						close(req.done)
					}
					return
				}
				if req.done != nil {
					close(req.done)
				}
			}
		}
	}()

	var mu sync.Mutex

	for {
		// Проверяем ошибки из горутин
		select {
		case err := <-errCh:
			cancel()
			return err
		default:
		}

		items, cookie, err := p.Next()
		if err != nil {
			if err == io.EOF {
				// Дожидаемся завершения активной обработки, чтобы сохранить порядок
				mu.Lock()
				if len(buf) > 0 {
					err = c.Process(buf)
					if err != nil {
						mu.Unlock()
						cancel()
						return fmt.Errorf("push error: %w", err)
					}
					// Отправляем куки в коммит-горутину и ждём подтверждения каждого
					for _, ck := range cookies {
						done := make(chan struct{})
						select {
						case <-ctx.Done():
							mu.Unlock()
							cancel()
							return context.Canceled
						case readyCookies <- commitReq{cookie: ck, done: done}:
						}
						select {
						case <-done:
							// Проверим возможную ошибку
							select {
							case e := <-errCh:
								mu.Unlock()
								cancel()
								return e
							default:
							}
						case e := <-errCh:
							mu.Unlock()
							cancel()
							return e
						}
					}
				}
				mu.Unlock()
				cancel()
				return io.EOF
			}
			cancel()
			return fmt.Errorf("read error: %w", err)
		}

		if len(buf)+len(items) <= MaxItems {
			buf = append(buf, items...)
			cookies = append(cookies, cookie)
			continue
		}

		// Переполнение: запускаем обработку в горутине под мьютексом
		toPush := append([]any(nil), buf...)
		toCommit := append([]int(nil), cookies...)
		mu.Lock()
		go func(pushBatch []any, commitCookies []int) {
			defer mu.Unlock()
			perr := c.Process(pushBatch)
			if perr != nil {
				select {
				case errCh <- fmt.Errorf("push error: %w", perr):
				default:
				}
				return
			}
			for _, ck := range commitCookies {
				select {
				case <-ctx.Done():
					return
				case readyCookies <- commitReq{cookie: ck}:
				}
			}
		}(toPush, toCommit)

		// Начинаем новый буфер с текущего батча
		buf = items
		cookies = []int{cookie}
	}
}
