package main

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProducer struct {
	batches   [][]any
	cookies   []int
	readErr   error
	callIndex int

	commitErrForCookie int
	commitErr          error

	commitAttempts []int
	committed      []int
}

func (m *mockProducer) Next() (items []any, cookie int, err error) {
	if m.callIndex < len(m.batches) {
		items = m.batches[m.callIndex]
		cookie = m.cookies[m.callIndex]
		m.callIndex++
		return items, cookie, nil
	}
	return nil, 0, m.readErr
}

func (m *mockProducer) Commit(cookie int) error {
	m.commitAttempts = append(m.commitAttempts, cookie)
	if m.commitErr != nil && cookie == m.commitErrForCookie {
		return m.commitErr
	}
	m.committed = append(m.committed, cookie)
	return nil
}

type mockConsumer struct {
	processed [][]any
	procErr   error
}

func (m *mockConsumer) Process(items []any) error {
	m.processed = append(m.processed, append([]any(nil), items...))
	if m.procErr != nil {
		return m.procErr
	}
	return nil
}

func makeItems(start, count int) []any {
	items := make([]any, count)
	for i := 0; i < count; i++ {
		items[i] = start + i
	}
	return items
}

func concat(a, b []any) []any {
	res := make([]any, 0, len(a)+len(b))
	res = append(res, a...)
	return append(res, b...)
}

func TestPipe_Success_BatchingAndCommitOrder(t *testing.T) {
	var err error
	firstBatchSize := MaxItems / 2
	secondBatchSize := MaxItems - firstBatchSize
	thirdBatchSize := 1

	batch1 := makeItems(0, firstBatchSize)
	batch2 := makeItems(firstBatchSize, secondBatchSize)
	batch3 := makeItems(firstBatchSize+secondBatchSize, thirdBatchSize)

	p := &mockProducer{
		batches: [][]any{batch1, batch2, batch3},
		cookies: []int{1, 2, 3},
		readErr: io.EOF,
	}
	c := &mockConsumer{}

	err = Pipe(p, c)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF), "ожидался io.EOF, получено: %v", err)

	require.Len(t, c.processed, 2, "ожидались два вызова Process")

	expectedProcessedFirst := concat(batch1, batch2)
	assert.True(t, reflect.DeepEqual(c.processed[0], expectedProcessedFirst), "несовпадение первого батча: получено %d элементов, ожидалось %d", len(c.processed[0]), len(expectedProcessedFirst))

	expectedProcessedSecond := batch3
	assert.True(t, reflect.DeepEqual(c.processed[1], expectedProcessedSecond), "несовпадение второго батча: получено %d элементов, ожидалось %d", len(c.processed[1]), len(expectedProcessedSecond))

	expectedCommits := []int{1, 2, 3}
	assert.True(t, reflect.DeepEqual(p.committed, expectedCommits), "нарушен порядок коммитов: получено %v, ожидалось %v", p.committed, expectedCommits)

	assert.Equal(t, len(expectedCommits), len(p.commitAttempts), "неожиданное число попыток коммита")
}

func TestPipe_ReadError(t *testing.T) {
	var err error
	p := &mockProducer{readErr: io.ErrUnexpectedEOF}
	c := &mockConsumer{}

	err = Pipe(p, c)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF), "ожидалась ошибка чтения io.ErrUnexpectedEOF, получено: %v", err)
	assert.Len(t, c.processed, 0, "не должно быть вызовов Process")
	assert.Len(t, p.commitAttempts, 0, "не должно быть вызовов Commit")
}

func TestPipe_ProcessError(t *testing.T) {
	var err error
	firstBatchSize := MaxItems / 2
	secondBatchSize := MaxItems - firstBatchSize

	p := &mockProducer{
		batches: [][]any{
			makeItems(0, firstBatchSize),
			makeItems(firstBatchSize, secondBatchSize+1), // overflow triggers Process
		},
		cookies: []int{1, 2},
		readErr: io.EOF,
	}
	c := &mockConsumer{procErr: errors.New("process failed")}

	err = Pipe(p, c)
	require.Error(t, err)
	require.True(t, errors.Is(err, c.procErr), "ожидалась ошибка обработки, получено: %v", err)
	assert.Len(t, p.commitAttempts, 0, "не должно быть вызовов Commit при ошибке Process")
}

func TestPipe_CommitError(t *testing.T) {
	var err error
	firstBatchSize := MaxItems / 2
	secondBatchSize := MaxItems - firstBatchSize

	p := &mockProducer{
		batches: [][]any{
			makeItems(0, firstBatchSize),
			makeItems(firstBatchSize, secondBatchSize+1), // overflow triggers Process
		},
		cookies:            []int{1, 2},
		readErr:            io.EOF,
		commitErrForCookie: 1,
		commitErr:          errors.New("commit failed"),
	}
	c := &mockConsumer{}

	err = Pipe(p, c)
	require.Error(t, err)
	require.True(t, errors.Is(err, p.commitErr), "ожидалась ошибка коммита, получено: %v", err)
	// Must attempt first commit and stop on failing cookie
	expectedAttempts := []int{1}
	assert.True(t, reflect.DeepEqual(p.commitAttempts, expectedAttempts), "несовпадение попыток коммита: получено %v, ожидалось %v", p.commitAttempts, expectedAttempts)
	// No successful commits
	assert.Len(t, p.committed, 0, "не должно быть успешных коммитов")
}
