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

func cloneAppend(bz []byte, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}

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

func readPlainFile(input io.Reader, fn func(version int64, changeSet *iavl.ChangeSet) (bool, error), parseChangeset bool) (uint64, error) {
	var (
		err             error
		written         int64
		versionHeader   [16]byte
		lastValidOffset uint64
	)

	cont := true
	lastValidOffset = 0
	for cont {
		if _, err = io.ReadFull(input, versionHeader[:]); err != nil {
			break
		}
		version := binary.LittleEndian.Uint64(versionHeader[:8])
		size := uint64(binary.LittleEndian.Uint64(versionHeader[8:16]))
		var changeSet iavl.ChangeSet
		if size > 0 {
			if parseChangeset {
				var (
					lenBuf [4]byte
					offset uint64
				)
				for offset < size {
					if _, err = io.ReadFull(input, lenBuf[:2]); err != nil {
						break
					}
					key := make([]byte, binary.LittleEndian.Uint16(lenBuf[:2]))
					if _, err = io.ReadFull(input, key); err != nil {
						break
					}
					if _, err = io.ReadFull(input, lenBuf[:]); err != nil {
						break
					}
					pair := iavl.KVPair{
						Key: key,
					}
					valueLen := binary.LittleEndian.Uint32(lenBuf[:])
					if valueLen == 0 {
						pair.Delete = true
					} else {
						value := make([]byte, valueLen)
						if _, err = io.ReadFull(input, value); err != nil {
							break
						}
						pair.Value = value
					}

					changeSet.Pairs = append(changeSet.Pairs, pair)
					offset += 2 + uint64(len(key)) + 4 + uint64(valueLen)
				}
				if err != nil {
					break
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
