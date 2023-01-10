package memiavl

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

func WriteSnapshot(root Node, version int64, nodesFile, keysFile, valuesFile, metadataFile string) error {
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

	fpMetadata, err := os.OpenFile(metadataFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer fpMetadata.Close()

	w := newSnapshotWriter(nodesWriter, keysWriter, valuesWriter)
	rootIndex, _, err := w.writeRecursive(root)
	if err != nil {
		return err
	}

	// write metadata
	var metadataBuf [16]byte
	binary.LittleEndian.PutUint64(metadataBuf[0:], uint64(version))
	binary.LittleEndian.PutUint64(metadataBuf[8:], rootIndex)
	if _, err := fpMetadata.Write(metadataBuf[:]); err != nil {
		return err
	}

	if err := fpKeys.Sync(); err != nil {
		return err
	}
	if err := fpValues.Sync(); err != nil {
		return err
	}
	if err := fpNodes.Sync(); err != nil {
		return err
	}
	return fpMetadata.Sync()
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
		binary.LittleEndian.PutUint32(buf[OffsetRight:], uint32(nodeIndex))

		nodeIndex, minimalKeyOffset, err = w.writeRecursive(node.Left())
		if err != nil {
			return 0, 0, err
		}
		binary.LittleEndian.PutUint32(buf[OffsetLeft:], uint32(nodeIndex))
	}

	copy(buf[OffsetHash:OffsetHash+SizeHash], node.Hash())
	if _, err := w.nodesWriter.Write(buf[:]); err != nil {
		return 0, 0, err
	}

	i := w.nodesIndex
	w.nodesIndex++
	return i, minimalKeyOffset, nil
}

func loadSnapshot(snapshotDir string) (*PersistedBlobs, uint64, uint64, error) {
	nodesFile := filepath.Join(snapshotDir, "nodes")
	keysFile := filepath.Join(snapshotDir, "keys")
	valuesFile := filepath.Join(snapshotDir, "values")
	metadataFile := filepath.Join(snapshotDir, "metadata")

	bz, err := os.ReadFile(metadataFile)
	if err != nil {
		return nil, 0, 0, err
	}
	version := binary.LittleEndian.Uint64(bz[:])
	rootIndex := binary.LittleEndian.Uint64(bz[8:])

	fpNodes, err := os.Open(nodesFile)
	if err != nil {
		return nil, 0, 0, err
	}

	nodes, nodesHandle, err := Mmap(fpNodes)
	if err != nil {
		fpNodes.Close()
		return nil, 0, 0, err
	}

	if len(nodes) == 0 {
		// empty tree
		if rootIndex != 0 {
			return nil, 0, 0, errors.New("corrupted snapshot, nodes are empty but rootIndex is not zero")
		}
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		return nil, 0, 0, nil
	}

	if uint64(len(nodes)) != (rootIndex+1)*SizeNode {
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		return nil, 0, 0, fmt.Errorf("nodes file size %d don't match root node index %d", len(nodes), rootIndex)
	}

	fpKeys, err := os.Open(keysFile)
	if err != nil {
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		return nil, 0, 0, err
	}
	keys, keysHandle, err := Mmap(fpKeys)
	if err != nil {
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		mmap.Munmap(keys, keysHandle)
		fpKeys.Close()
		return nil, 0, 0, err
	}

	fpValues, err := os.Open(valuesFile)
	if err != nil {
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		mmap.Munmap(keys, keysHandle)
		fpKeys.Close()
		return nil, 0, 0, err
	}
	values, valuesHandle, err := Mmap(fpValues)
	if err != nil {
		mmap.Munmap(nodes, nodesHandle)
		fpNodes.Close()
		mmap.Munmap(keys, keysHandle)
		fpKeys.Close()
		mmap.Munmap(values, valuesHandle)
		fpValues.Close()
		return nil, 0, 0, err
	}

	blobs := &PersistedBlobs{
		nodesFile:  fpNodes,
		keysFile:   fpKeys,
		valuesFile: fpValues,

		nodes:  nodes,
		keys:   keys,
		values: values,

		nodesHandle:  nodesHandle,
		keysHandle:   keysHandle,
		valuesHandle: valuesHandle,
	}

	return blobs, version, rootIndex, nil
}

func Mmap(f *os.File) ([]byte, *[mmap.MaxMapSize]byte, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	return mmap.Mmap(f, int(fi.Size()))
}
