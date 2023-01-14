package outersort

type LesserFunc func(a, b []byte) bool
type NextFunc func() ([]byte, error)
