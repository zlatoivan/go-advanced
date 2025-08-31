package main

import (
	"errors"
	"io"
	"strings"
)

var privateTestCases = []TestCase{
	{
		name: "Seek от конца",
		run: func() bool {
			a := newMockStringsReader("abc")
			b := newMockStringsReader("def")
			m := NewMultiReader(4, a, b)

			pos, err := m.Seek(-2, io.SeekEnd)
			if err != nil || pos != 4 {
				return false
			}

			buf := make([]byte, 2)
			n, err := m.Read(buf)
			if err != nil || n != 2 {
				return false
			}
			return string(buf) == "ef"
		},
	},
	{
		name: "Seek от текущей позиции",
		run: func() bool {
			a := newMockStringsReader("abcd")
			m := NewMultiReader(4, a)

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if err != nil || n != 1 || string(buf) != "a" {
				return false
			}

			pos, err := m.Seek(2, io.SeekCurrent)
			if err != nil || pos != 3 {
				return false
			}

			n, err = m.Read(buf)
			if err != nil || n != 1 {
				return false
			}
			return string(buf) == "d"
		},
	},
	{
		name: "Ошибочные варианты Seek",
		run: func() bool {
			a := newMockStringsReader("abc")
			m := NewMultiReader(4, a)

			if _, err := m.Seek(0, 99); err == nil {
				return false
			}
			if _, err := m.Seek(-1, io.SeekStart); err == nil {
				return false
			}
			if _, err := m.Seek(5, io.SeekStart); err == nil {
				return false
			}
			return true
		},
	},
	{
		name: "Close агрегирует ошибки",
		run: func() bool {
			errA := errors.New("A")
			errB := errors.New("B")
			a := newMockStringsReader("x")
			b := newMockStringsReader("y")
			c := newMockStringsReader("z")
			a.closeErr = errA
			b.closeErr = errB

			m := NewMultiReader(4, a, b, c)

			err := m.Close()
			if err == nil {
				return false
			}
			if !errors.Is(err, errA) || !errors.Is(err, errB) {
				return false
			}
			return a.closed && b.closed && c.closed
		},
	},
	{
		name: "Read/Seek после Close",
		run: func() bool {
			a := newMockStringsReader("abc")
			m := NewMultiReader(4, a)

			err := m.Close()
			if err != nil {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if n != 0 || !errors.Is(err, io.ErrClosedPipe) {
				return false
			}

			if _, err = m.Seek(0, io.SeekStart); !errors.Is(err, io.ErrClosedPipe) {
				return false
			}

			err = m.Close()
			return err == nil
		},
	},
	{
		name: "Size кэшируется и не пересчитывается",
		run: func() bool {
			var calls int
			tr1 := newMockStringsReader(strings.Repeat("a", 2))
			tr2 := newMockStringsReader(strings.Repeat("b", 3))
			tr1.sizeCalls = &calls
			tr2.sizeCalls = &calls

			m := NewMultiReader(4, tr1, tr2)
			if calls != 2 {
				return false
			}
			_ = m.Size()
			_ = m.Size()
			return calls == 2
		},
	},
	{
		name: "Ленивый Seek выполняется при первом чтении",
		run: func() bool {
			var seekCalls1, seekCalls2 int
			tr1 := newMockStringsReader("abc")
			tr2 := newMockStringsReader("def")
			tr1.seekCalls = &seekCalls1
			tr2.seekCalls = &seekCalls2

			m := NewMultiReader(4, tr1, tr2)

			pos, err := m.Seek(4, io.SeekStart)
			if err != nil || pos != 4 {
				return false
			}
			if seekCalls1 != 0 || seekCalls2 != 0 {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if err != nil || n != 1 || string(buf) != "e" {
				return false
			}
			if seekCalls1 != 0 {
				return false
			}
			return seekCalls2 > 0
		},
	},
	{
		name: "Seek на EOF допустим и Read возвращает EOF",
		run: func() bool {
			a := newMockStringsReader("data")
			m := NewMultiReader(4, a)

			size := m.Size()
			pos, err := m.Seek(0, io.SeekEnd)
			if err != nil || pos != size {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if n != 0 {
				return false
			}
			return errors.Is(err, io.EOF)
		},
	},
	{
		name: "Read с нулевой длиной возвращает (0, nil)",
		run: func() bool {
			a := newMockStringsReader("xy")
			m := NewMultiReader(4, a)
			n, err := m.Read(nil)
			return n == 0 && err == nil
		},
	},
	{
		name: "Seek внутри буферного окна не вызывает нижний Seek",
		run: func() bool {
			var seekCalls int
			a := newMockStringsReader("hello world")
			a.seekCalls = &seekCalls
			m := NewMultiReader(4, a)
			buf := make([]byte, 1)
			// Старт чтения, префетчер станет активным и сделает первый Seek
			if n, err := m.Read(buf); err != nil || n != 1 {
				return false
			}
			before := seekCalls
			// Переход вперёд на 1 байт — должен быть внутри уже буферизованного окна
			if _, err := m.Seek(1, io.SeekCurrent); err != nil {
				return false
			}
			// Следующее чтение должно прийти из буфера, без новых Seek в источнике
			if n, err := m.Read(buf); err != nil || n != 1 {
				return false
			}
			return seekCalls == before
		},
	},
	{
		name: "Seek назад за пределы окна инициирует новый нижний Seek",
		run: func() bool {
			var seekCalls int
			a := newMockStringsReader("longstringdata")
			a.seekCalls = &seekCalls
			m := NewMultiReader(4, a)
			buf := make([]byte, 5)
			if n, err := m.Read(buf); err != nil || n != 5 { // прочитаем немного вперёд
				return false
			}
			before := seekCalls
			// Сильно назад — за границы текущего окна (bufferStart уже сдвинут вперёд)
			if _, err := m.Seek(0, io.SeekStart); err != nil {
				return false
			}
			// Первое же чтение должно потребовать нижний Seek
			if n, err := m.Read(buf[:1]); err != nil || n != 1 {
				return false
			}
			return seekCalls > before
		},
	},
}
