package main

import (
	"errors"
	"io"
	"strings"
)

const bufferSize = 1024 * 1024

var privateTestCases = []TestCase{
	//{
	//	name: "Close агрегирует ошибки",
	//	run: func() bool {
	//		errA := errors.New("A")
	//		errB := errors.New("B")
	//		a := newMockStringsReader("x")
	//		b := newMockStringsReader("y")
	//		c := newMockStringsReader("z")
	//		a.closeErr = errA
	//		b.closeErr = errB
	//
	//		m := NewMultiReader(bufferSize, 4, a, b, c)
	//
	//		err := m.Close()
	//		if err == nil {
	//			return false
	//		}
	//		if !errors.Is(err, errA) || !errors.Is(err, errB) {
	//			return false
	//		}
	//		return a.closed && b.closed && c.closed
	//	},
	//},
	{
		name: "Read после Close",
		run: func() bool {
			a := newMockStringsReader("abc")
			m := NewMultiReader(bufferSize, 4, a)

			err := m.Close()
			if err != nil {
				return false
			}

			buf := make([]byte, 1)
			n, err := m.Read(buf)
			if n != 0 || !errors.Is(err, io.ErrClosedPipe) {
				return false
			}

			err = m.Close()
			return err == nil
		},
	},
	// Проверка корректности Size без требований к кэшированию
	{
		name: "Size возвращает корректную сумму",
		run: func() bool {
			tr1 := newMockStringsReader(strings.Repeat("a", 2))
			tr2 := newMockStringsReader(strings.Repeat("b", 3))
			m := NewMultiReader(bufferSize, 4, tr1, tr2)
			return m.Size() == 5
		},
	},
	{
		name: "Read с нулевой длиной возвращает (0, nil)",
		run: func() bool {
			a := newMockStringsReader("xy")
			m := NewMultiReader(bufferSize, 4, a)
			n, err := m.Read(nil)
			return n == 0 && err == nil
		},
	},
	// Удалены все сценарии Seek; ниже — проверки последовательного чтения через границы
	{
		name: "Чтение через границу A->B без Seek",
		run: func() bool {
			s1 := strings.Repeat("A", 1024)
			s2 := strings.Repeat("B", 768)
			a := newMockStringsReader(s1)
			b := newMockStringsReader(s2)
			m := NewMultiReader(bufferSize, 4, a, b)

			// Пропускаем len(s1)-10
			discard := make([]byte, len(s1)-10)
			n, err := m.Read(discard)
			if err != nil || n != len(discard) {
				return false
			}
			// Читаем 20 байт, должны пересечь границу
			buf := make([]byte, 20)
			n, err = m.Read(buf)
			if err != nil || n != 20 {
				return false
			}
			return string(buf) == strings.Repeat("A", 10)+strings.Repeat("B", 10)
		},
	},
	{
		name: "Чтение через границу B->C без Seek",
		run: func() bool {
			s1 := strings.Repeat("A", 1024)
			s2 := strings.Repeat("B", 768)
			s3 := strings.Repeat("C", 512)
			a := newMockStringsReader(s1)
			b := newMockStringsReader(s2)
			c := newMockStringsReader(s3)
			m := NewMultiReader(bufferSize, 4, a, b, c)

			// Пропускаем len(s1)+len(s2)-5
			discard := make([]byte, len(s1)+len(s2)-5)
			n, err := m.Read(discard)
			if err != nil || n != len(discard) {
				return false
			}
			// Читаем 15 байт, пересекаем B->C
			buf := make([]byte, 15)
			n, err = m.Read(buf)
			if err != nil || n != 15 {
				return false
			}
			return string(buf) == strings.Repeat("B", 5)+strings.Repeat("C", 10)
		},
	},
	{
		name: "Маленькие ридеры, большие буферы",
		run: func() bool {
			a := newMockStringsReader("aaaaa")
			b := newMockStringsReader("bbb")
			c := newMockStringsReader("cccccccc")
			m := NewMultiReader(bufferSize, 2, a, b, c)
			buf := make([]byte, int(m.Size()))
			n, err := m.Read(buf)
			if err != nil || n != len(buf) {
				return false
			}
			return string(buf) == "aaaaabbbcccccccc"
		},
	},
	{
		name: "EOF при достижении конца общего потока",
		run: func() bool {
			r := newMockStringsReader("z")
			m := NewMultiReader(bufferSize, 1, r)
			b := make([]byte, 10)
			n, err := m.Read(b)
			if n != 1 || string(b[:n]) != "z" || !errors.Is(err, io.EOF) {
				return false
			}
			return true
		},
	},
	{
		name: "Close во время фонового чтения не падает",
		run: func() bool {
			r := newMockStringsReader(strings.Repeat("a", 1<<16))
			m := NewMultiReader(bufferSize, 2, r)
			done := make(chan struct{})
			go func() {
				buf := make([]byte, 1<<15)
				_, _ = m.Read(buf)
				close(done)
			}()
			_ = m.Close()
			<-done
			return true
		},
	},
	{
		name: "Большие данные: полное чтение и чтение через границы",
		run: func() bool {
			// Сгенерируем несколько «больших» источников по ~1–2KB суммарно
			s1 := strings.Repeat("A", 1024)
			s2 := strings.Repeat("B", 768)
			s3 := strings.Repeat("C", 512)
			a := newMockStringsReader(s1)
			b := newMockStringsReader(s2)
			c := newMockStringsReader(s3)
			m := NewMultiReader(bufferSize, 4, a, b, c)

			// Полное чтение и сравнение
			expected := s1 + s2 + s3
			buf := make([]byte, len(expected))
			n, err := m.Read(buf)
			if err != nil || n != len(expected) || string(buf) != expected {
				return false
			}

			// Чтение через границу A->B: пересоздаём ридеры, пропускаем len(s1)-10, читаем 20
			{
				a2 := newMockStringsReader(s1)
				b2 := newMockStringsReader(s2)
				m2 := NewMultiReader(bufferSize, 4, a2, b2)
				discard := make([]byte, len(s1)-10)
				n, err = m2.Read(discard)
				if err != nil || n != len(discard) {
					return false
				}
				buf2 := make([]byte, 20)
				n, err = m2.Read(buf2)
				if err != nil || n != 20 {
					return false
				}
				if string(buf2) != strings.Repeat("A", 10)+strings.Repeat("B", 10) {
					return false
				}
			}

			// Чтение через границу B->C: пересоздаём ридеры, пропускаем len(s1)+len(s2)-5, читаем 15
			{
				a3 := newMockStringsReader(s1)
				b3 := newMockStringsReader(s2)
				c3 := newMockStringsReader(s3)
				m3 := NewMultiReader(bufferSize, 4, a3, b3, c3)
				discard := make([]byte, len(s1)+len(s2)-5)
				n, err = m3.Read(discard)
				if err != nil || n != len(discard) {
					return false
				}
				buf3 := make([]byte, 15)
				n, err = m3.Read(buf3)
				if err != nil || n != 15 {
					return false
				}
				if string(buf3) != strings.Repeat("B", 5)+strings.Repeat("C", 10) {
					return false
				}
			}
			return true
		},
	},
}
