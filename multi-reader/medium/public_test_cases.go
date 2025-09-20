package main

import (
	"errors"
	"io"
)

// TestCase описывает один самостоятельный тест: имя и функцию проверки.
type TestCase struct {
	name string
	run  func() bool
}

var testCases = []TestCase{
	{
		name: "Size и последовательное чтение",
		run: func() bool {
			a := newMockStringsReader("abc")
			b := newMockStringsReader("defg")
			m := NewMultiReader(bufferSize, 4, a, b)

			if m.Size() != int64(7) {
				return false
			}

			buf := make([]byte, 7)
			n, err := m.Read(buf)
			if err != nil {
				return false
			}
			if n != 7 {
				return false
			}
			return string(buf) == "abcdefg"
		},
	},
	{
		name: "Поведение EOF",
		run: func() bool {
			a := newMockStringsReader("hi")
			m := NewMultiReader(bufferSize, 4, a)
			buf := make([]byte, 2)

			n, err := m.Read(buf)
			if err != nil || n != 2 || string(buf) != "hi" {
				return false
			}

			n, err = m.Read(buf)
			if n != 0 {
				return false
			}
			return errors.Is(err, io.EOF)
		},
	},
	{
        name: "Чтение после пропуска первых байт последовательным чтением",
		run: func() bool {
			a := newMockStringsReader("hello")
			b := newMockStringsReader("-world-")
			m := NewMultiReader(bufferSize, 4, a, b)

            // Пропустим первые 3 байта последовательным чтением
            skip := make([]byte, 3)
            n, err := m.Read(skip)
            if err != nil || n != 3 {
                return false
            }

            buf := make([]byte, 5)
            n, err = m.Read(buf)
			if err != nil {
				return false
			}
			return string(buf[:n]) == "lo-wo"
		},
	},
}
