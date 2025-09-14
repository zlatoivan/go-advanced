package main

import "fmt"

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

	for {
		items, cookie, err := p.Next()
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		if len(buf)+len(items) <= MaxItems {
			buf = append(buf, items...)
			cookies = append(cookies, cookie)
			continue
		}

		err = c.Process(buf)
		if err != nil {
			return fmt.Errorf("push error: %w", err)
		}
		for _, ck := range cookies {
			err = p.Commit(ck)
			if err != nil {
				return fmt.Errorf("error commiting cookie %d: %w", ck, err)
			}
		}

		buf = items
		cookies = []int{cookie}
	}
}
