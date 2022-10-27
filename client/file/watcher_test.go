package file

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupDirectory(t *testing.T, directory string) func(t *testing.T) {
	err := os.MkdirAll(directory, os.ModePerm)
	require.NoError(t, err)
	fmt.Println("setup directory:", directory)
	return func(t *testing.T) {
		os.RemoveAll(directory)
		fmt.Println("cleanup directory")
	}
}

func setupBlockFiles(directory string, start, end int) {
	for i := start; i <= end; i++ {
		file := GetLocalDataFileName(directory, i)
		os.WriteFile(file, []byte(fmt.Sprint("block", i)), 0600)
	}
}

func start(watcher *BlockFileWatcher, startBlockNum, endBlockNum int) int {
	watcher.Start(startBlockNum, time.Microsecond)
	counter := 0
	for data := range watcher.SubscribeData() {
		if data != nil && len(data.Data) > 0 {
			counter++
		}
		if data.BlockNum == endBlockNum {
			watcher.Close()
			return counter
		}
	}
	return counter
}

func TestFileWatcher(t *testing.T) {
	directory := "tmp"
	teardown := setupDirectory(t, directory)
	startBlockNum := 1
	endBlockNum := 2
	concurrency := 1
	defer teardown(t)

	t.Run("when sync via local", func(t *testing.T) {
		setupBlockFiles(directory, startBlockNum, endBlockNum)
		watcher := NewBlockFileWatcher(concurrency, endBlockNum+1, func(blockNum int) string {
			return GetLocalDataFileName(directory, blockNum)
		}, true)
		total := start(watcher, startBlockNum, endBlockNum)
		expected := endBlockNum - startBlockNum + 1
		require.Equal(t, expected, total)
	})

	t.Run("when sync via http", func(t *testing.T) {
		setupBlockFiles(directory, startBlockNum, endBlockNum)
		http.Handle("/", http.FileServer(http.Dir(directory)))
		port := "8080"
		fmt.Printf("Serving %s on HTTP port: %s\n", directory, port)
		go func() {
			log.Fatal(http.ListenAndServe(":"+port, nil))
		}()
		watcher := NewBlockFileWatcher(concurrency, endBlockNum+1, func(blockNum int) string {
			return fmt.Sprintf("http://localhost:%s/%s", port, DataFileName(blockNum))
		}, false)
		total := start(watcher, startBlockNum, endBlockNum)
		expected := endBlockNum - startBlockNum + 1
		require.Equal(t, expected, total)
	})
}
