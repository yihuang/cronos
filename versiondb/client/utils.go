package client

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"io"
	"os"
	"strings"

	"github.com/crypto-org-chain/cronos/versiondb"
	"github.com/gogo/protobuf/proto"
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
		reader = bufio.NewReader(fp)
		if strings.HasSuffix(plainFile, CompressedFileSuffix) {
			zreader, err := zlib.NewReader(reader)
			if err != nil {
				return err
			}
			reader = zreader
		}
	}
	return fn(reader)
}

func readPlainFile(input io.Reader, fn func(version int64, changeSet *versiondb.ChangeSet) (bool, error), parseChangeset bool) (int, error) {
	var (
		err             error
		written         int64
		versionHeader   [16]byte
		lastValidOffset int
	)

	cont := true
	lastValidOffset = 0
	for cont {
		if _, err = io.ReadFull(input, versionHeader[:]); err != nil {
			break
		}
		version := binary.LittleEndian.Uint64(versionHeader[:8])
		size := int(binary.LittleEndian.Uint64(versionHeader[8:16]))
		var changeSet versiondb.ChangeSet
		if size > 0 {
			if parseChangeset {
				buf := make([]byte, size)
				if _, err = io.ReadFull(input, buf); err != nil {
					break
				}

				if err = proto.Unmarshal(buf, &changeSet); err != nil {
					return lastValidOffset, err
				}
			} else {
				if written, err = io.CopyN(io.Discard, input, int64(size)); err != nil {
					if err == io.EOF && written < int64(size) {
						err = io.ErrUnexpectedEOF
					}
					break
				}
			}
		}

		cont, err = fn(int64(version), &changeSet)
		if err != nil {
			return lastValidOffset, err
		}

		lastValidOffset += size + 16
	}

	if err != io.EOF {
		return lastValidOffset, err
	}

	return lastValidOffset, nil
}
