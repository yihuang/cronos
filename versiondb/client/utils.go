package client

import (
	"bufio"
	"compress/zlib"
	"encoding/binary"
	"io"
	"os"
	"strings"

	"github.com/cosmos/iavl"
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

func readPlainFile(input io.Reader, fn func(version int64, changeSet *iavl.ChangeSet) (bool, error), parseChangeset bool) (int, error) {
	var (
		err             error
		written         int64
		versionHeader   [16]byte
		lastValidOffset int
	)

	seeker, isSeeker := input.(io.Seeker)

	cont := true
	lastValidOffset = 0
	for cont {
		if _, err = io.ReadFull(input, versionHeader[:]); err != nil {
			break
		}
		version := binary.LittleEndian.Uint64(versionHeader[:8])
		size := int(binary.LittleEndian.Uint64(versionHeader[8:16]))
		var changeSet iavl.ChangeSet
		if size > 0 {
			if parseChangeset {
				var lenBuf [4]byte
				for {
					if _, err := input.Read(lenBuf[:2]); err != nil {
						return lastValidOffset, err
					}
					key := make([]byte, binary.LittleEndian.Uint16(lenBuf[:2]))
					if _, err := input.Read(key); err != nil {
						return lastValidOffset, err
					}
					if _, err := input.Read(lenBuf[:]); err != nil {
						return lastValidOffset, err
					}
					pair := iavl.KVPair{
						Key: key,
					}
					valueLen := binary.LittleEndian.Uint32(lenBuf[:])
					if valueLen == 0 {
						pair.Delete = true
					} else {
						value := make([]byte, valueLen)
						if _, err := input.Read(value); err != nil {
							return lastValidOffset, err
						}
						pair.Value = value
					}

					changeSet.Pairs = append(changeSet.Pairs, pair)
				}
			} else if isSeeker {
				if _, err := seeker.Seek(int64(size), io.SeekCurrent); err != nil {
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
