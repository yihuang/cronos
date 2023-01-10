package memiavl

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

func WriteSnapshot(root Node, nodesFile, keysFile, valuesFile string) error {
	fpNodes, err := os.OpenFile(nodesFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer fpNodes.Close()
	nodesWriter := bufio.NewWriter(fpNodes)
	defer nodesWriter.Flush()

	fpKeys, err := os.OpenFile(keysFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer fpKeys.Close()
	keysWriter := bufio.NewWriter(fpKeys)
	defer keysWriter.Flush()

	fpValues, err := os.OpenFile(valuesFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer fpValues.Close()
	valuesWriter := bufio.NewWriter(fpValues)
	defer valuesWriter.Flush()

	w := newSnapshotWriter(nodesWriter, keysWriter, valuesWriter)
	_, _, err = w.writeRecursive(root)
	return err
}

type snapshotWriter struct {
	nodesWriter, keysWriter, valuesWriter io.Writer
	nodesIndex, keysOffset, valuesOffset  uint64
}

func newSnapshotWriter(nodesWriter, keysWriter, valuesWriter io.Writer) *snapshotWriter {
	return &snapshotWriter{
		nodesWriter:  nodesWriter,
		keysWriter:   keysWriter,
		valuesWriter: valuesWriter,
	}
}

// max key length: 2**16
func (w *snapshotWriter) writeKey(key []byte) (uint64, error) {
	var buf [2]byte
	// TODO check overflow
	binary.LittleEndian.PutUint16(buf[:], uint16(len(key)))
	if _, err := w.keysWriter.Write(buf[:]); err != nil {
		return 0, err
	}
	if _, err := w.keysWriter.Write(key); err != nil {
		return 0, err
	}
	offset := w.keysOffset
	w.keysOffset += uint64(len(key)) + 2
	return offset, nil
}

func (w *snapshotWriter) writeValue(value []byte) (uint64, error) {
	var buf [4]byte
	// TODO check overflow
	binary.LittleEndian.PutUint32(buf[:], uint32(len(value)))
	if _, err := w.valuesWriter.Write(buf[:]); err != nil {
		return 0, err
	}
	if _, err := w.valuesWriter.Write(value); err != nil {
		return 0, err
	}
	offset := w.valuesOffset
	w.valuesOffset += uint64(len(value)) + 4
	return offset, nil
}

// writeRecursive write the node recursively in depth-first post-order,
// returns `(nodeIndex, offset of minimal key in subtree, err)`.
func (w *snapshotWriter) writeRecursive(node Node) (uint64, uint64, error) {
	var (
		buf              [SizeNode]byte
		minimalKeyOffset uint64
	)

	buf[OffsetHeight] = byte(node.Height())
	// TODO overflow check
	binary.LittleEndian.PutUint32(buf[OffsetVersion:], uint32(node.Version()))
	binary.LittleEndian.PutUint64(buf[OffsetSize:], uint64(node.Size()))

	if isLeaf(node) {
		offset, err := w.writeKey(node.Key())
		if err != nil {
			return 0, 0, err
		}
		binary.LittleEndian.PutUint64(buf[OffsetKey:], offset)
		minimalKeyOffset = offset

		offset, err = w.writeValue(node.Value())
		if err != nil {
			return 0, 0, err
		}
		binary.LittleEndian.PutUint64(buf[OffsetValue:], offset)
	} else {
		// it use the minimal key from right subtree, but propogate the minimal key from left subtree.
		nodeIndex, keyOffset, err := w.writeRecursive(node.Right())
		if err != nil {
			return 0, 0, err
		}
		binary.LittleEndian.PutUint64(buf[OffsetKey:], keyOffset)
		binary.LittleEndian.PutUint64(buf[OffsetRight:], nodeIndex)

		nodeIndex, minimalKeyOffset, err = w.writeRecursive(node.Left())
		if err != nil {
			return 0, 0, err
		}
		binary.LittleEndian.PutUint64(buf[OffsetLeft:], nodeIndex)
	}

	copy(buf[OffsetHash:], node.Hash())
	if _, err := w.nodesWriter.Write(buf[:]); err != nil {
		return 0, 0, err
	}

	i := w.nodesIndex
	w.nodesIndex++
	return i, minimalKeyOffset, nil
}
