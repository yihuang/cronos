package extsort

type LesserFunc func(a, b []byte) bool
type NextFunc func() ([]byte, error)
