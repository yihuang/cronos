package extsort

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/golang/snappy"
)

type ExtSorter struct {
	// directory to store temporary chunk files
	tmpDir string
	// target chunk size
	chunkSize  int64
	lesserFunc LesserFunc

	// current chunk
	currentChunk     [][]byte
	currentChunkSize int64

	// manage the chunk goroutines
	chunkWG sync.WaitGroup
	lock    sync.Mutex
	// finished chunk files
	chunkFiles []*os.File
	// chunking goroutine failure messages
	failures []string
}

func New(tmpDir string, chunkSize int64, lesserFunc LesserFunc) *ExtSorter {
	return &ExtSorter{
		tmpDir:     tmpDir,
		chunkSize:  chunkSize,
		lesserFunc: lesserFunc,
	}
}

func (s *ExtSorter) Feed(item []byte) error {
	if len(item) > math.MaxUint32 {
		return errors.New("item length overflows uint32")
	}

	s.currentChunkSize += int64(len(item)) + 4
	s.currentChunk = append(s.currentChunk, item)

	if s.currentChunkSize >= s.chunkSize {
		return s.sortChunkAndRotate()
	}
	return nil
}

func (s *ExtSorter) sortChunkAndRotate() error {
	chunkFile, err := os.CreateTemp(s.tmpDir, "sort-chunk-*")
	if err != nil {
		return err
	}

	// rotate chunk
	chunk := s.currentChunk
	s.currentChunk = nil
	s.currentChunkSize = 0

	s.chunkWG.Add(1)
	go func() {
		defer s.chunkWG.Done()
		if err := sortAndSaveChunk(chunk, s.lesserFunc, chunkFile); err != nil {
			chunkFile.Close()
			s.lock.Lock()
			defer s.lock.Unlock()
			s.failures = append(s.failures, err.Error())
			return
		}
		s.lock.Lock()
		defer s.lock.Unlock()
		s.chunkFiles = append(s.chunkFiles, chunkFile)
	}()
	return nil
}

// Finalize wait for all chunking goroutines to finish, and return the merged stream.
func (s *ExtSorter) Finalize() (*MultiWayMerge, error) {
	// handle the pending chunk
	if s.currentChunkSize > 0 {
		if err := s.sortChunkAndRotate(); err != nil {
			return nil, err
		}
	}

	s.chunkWG.Wait()
	if len(s.failures) > 0 {
		return nil, errors.New(strings.Join(s.failures, "\n"))
	}

	streams := make([]NextFunc, len(s.chunkFiles))
	for i, chunkFile := range s.chunkFiles {
		if _, err := chunkFile.Seek(0, 0); err != nil {
			return nil, err
		}
		reader := snappy.NewReader(chunkFile)
		streams[i] = func() ([]byte, error) {
			item, err := readItem(reader)
			if err == io.EOF {
				return nil, nil
			}
			return item, err
		}
	}

	return NewMultiWayMerge(streams, s.lesserFunc)
}

// Close closes all the temporary files
func (s *ExtSorter) Close() error {
	var err error
	for _, chunkFile := range s.chunkFiles {
		err1 := chunkFile.Close()
		if err != nil {
			err = err1
		}
		err1 = os.Remove(chunkFile.Name())
		if err != nil {
			err = err1
		}
	}
	return err
}

func sortAndSaveChunk(chunk [][]byte, lesserFunc LesserFunc, output *os.File) error {
	// sort the chunk and write to file
	sort.Slice(chunk, func(i, j int) bool {
		return lesserFunc(chunk[i], chunk[j])
	})

	writer := snappy.NewBufferedWriter(output)
	var sizeBuf [4]byte
	for _, item := range chunk {
		binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(item)))
		if _, err := writer.Write(sizeBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(item); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func readItem(reader io.Reader) ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(reader, sizeBuf[:]); err != nil {
		return nil, err
	}
	item := make([]byte, binary.LittleEndian.Uint32(sizeBuf[:]))
	if _, err := io.ReadFull(reader, item); err != nil {
		return nil, err
	}
	return item, nil
}
