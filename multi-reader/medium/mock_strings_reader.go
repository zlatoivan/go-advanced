package main

import "strings"

// mockStringsReader — адаптер поверх strings.Reader, реализующий SizedReadCloser и позволяющий считать вызовы.
type mockStringsReader struct {
	*strings.Reader
	size      int64
	closed    bool
	closeErr  error
	sizeCalls *int
}

func newMockStringsReader(s string) *mockStringsReader {
	r := strings.NewReader(s)
	return &mockStringsReader{
		Reader: r,
		size:   int64(r.Len()),
	}
}

func (c *mockStringsReader) Read(p []byte) (int, error) {
	return c.Reader.Read(p)
}

func (c *mockStringsReader) Close() error {
	c.closed = true
	return c.closeErr
}

func (c *mockStringsReader) Size() int64 {
	if c.sizeCalls != nil {
		*c.sizeCalls++
	}
	return c.size
}
