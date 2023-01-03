package client

import (
	"compress/zlib"
	"io"
	"os"
	"strings"
)

const (
	CompressedFileSuffix = ".zz"
)

func withPlainInput(plainFile string, fn func(io.Reader) error) error {
	var reader io.Reader
	if plainFile == "-" {
		reader = os.Stdin
	} else {
		fp, err := os.Open(plainFile)
		if err != nil {
			return err
		}
		defer fp.Close()
		reader = fp
		if strings.HasSuffix(plainFile, CompressedFileSuffix) {
			zreader, err := zlib.NewReader(fp)
			if err != nil {
				return err
			}
			reader = zreader
		}
	}
	return fn(reader)
}
