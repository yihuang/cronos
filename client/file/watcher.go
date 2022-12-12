package file

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var errNotExist = errors.New("file not exist")

type fileDownloader interface {
	GetData(path string) ([]byte, error)
}

type localFileDownloader struct{}

func (d *localFileDownloader) GetData(path string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			err = errNotExist
		}
		return nil, err
	}
	return data, nil
}

type httpFileDownloader struct{}

func (d *httpFileDownloader) GetData(path string) ([]byte, error) {
	c := &http.Client{
		Timeout: time.Minute,
	}
	resp, err := c.Get(path) //nolint
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if resp.StatusCode == http.StatusNotFound {
			return nil, errNotExist
		}
		return nil, errors.New(resp.Status)
	}
	return ioutil.ReadAll(resp.Body)
}

type BlockData struct {
	BlockNum int
	Data     []byte
	ChResult chan<- error
}

type BlockFileWatcher struct {
	concurrency   int
	maxBlockNum   int
	getFilePath   func(blockNum int) string
	onBeforeFetch func(blockNum int) bool
	downloader    fileDownloader
	chData        chan *BlockData
	chError       chan error
	chDone        chan bool
	startLock     *sync.Mutex
	maxBlockLock  *sync.RWMutex
}

func NewBlockFileWatcher(
	concurrency int,
	maxBlockNum int,
	getFilePath func(blockNum int) string,
	onBeforeFetch func(blockNum int) bool,
	isLocal bool,
) *BlockFileWatcher {
	w := &BlockFileWatcher{
		concurrency:   concurrency,
		maxBlockNum:   maxBlockNum,
		getFilePath:   getFilePath,
		onBeforeFetch: onBeforeFetch,
		chData:        make(chan *BlockData),
		chError:       make(chan error),
		startLock:     new(sync.Mutex),
		maxBlockLock:  new(sync.RWMutex),
	}
	if isLocal {
		w.downloader = new(localFileDownloader)
	} else {
		w.downloader = new(httpFileDownloader)
	}
	return w
}

func DataFileName(blockNum int) string {
	return fmt.Sprintf("block-%d-data", blockNum)
}

func GetLocalDataFileName(directory string, blockNum int) string {
	return fmt.Sprintf("%s/%s", directory, DataFileName(blockNum))
}

func (w *BlockFileWatcher) SubscribeData() <-chan *BlockData {
	return w.chData
}

func (w *BlockFileWatcher) SubscribeError() <-chan error {
	return w.chError
}

func (w *BlockFileWatcher) SetMaxBlockNum(num int) {
	w.maxBlockLock.Lock()
	defer w.maxBlockLock.Unlock()
	// avoid dup job when set max to smaller one while locking
	if num > w.maxBlockNum {
		w.maxBlockNum = num
	}
}

func (w *BlockFileWatcher) fetch(blockNum int) error {
	if w.onBeforeFetch != nil && !w.onBeforeFetch(blockNum) {
		return nil
	}

	path := w.getFilePath(blockNum)
	data, err := w.downloader.GetData(path)
	if err != nil {
		if err != errNotExist {
			// avoid blocked by error when not subscribe
			select {
			case w.chError <- err:
			default:
			}
		}
		return err
	}

	chResult := make(chan error)
	w.chData <- &BlockData{
		BlockNum: blockNum,
		Data:     data,
		ChResult: chResult,
	}
	return <-chResult
}

func (w *BlockFileWatcher) Start(
	blockNum int,
	interval time.Duration,
) {
	w.startLock.Lock()
	defer w.startLock.Unlock()
	if w.chDone != nil {
		return
	}
	w.chDone = make(chan bool)
	go func() {
		finishedBlockNums := make(map[int]bool)
		for {
			select {
			case <-w.chDone:
				return

			default:
				wg := new(sync.WaitGroup)
				currentBlockNum := blockNum
				w.maxBlockLock.RLock()
				maxBlockNum := w.maxBlockNum
				w.maxBlockLock.RUnlock()
				concurrency := w.concurrency
				if diff := maxBlockNum - currentBlockNum; diff < concurrency {
					if diff <= 0 {
						time.Sleep(interval)
						break
					}
					concurrency = diff
				}
				resultErrs := make([]error, concurrency)
				for i := 0; i < concurrency; i++ {
					nextBlockNum := blockNum + i
					fmt.Println("mm-start: ", nextBlockNum)
					if !finishedBlockNums[nextBlockNum] {
						wg.Add(1)
						go func(nextBlockNum, i int) {
							err := w.fetch(nextBlockNum)
							resultErrs[i] = err
							wg.Done()
						}(nextBlockNum, i)
					}
				}
				wg.Wait()
				errReached := false
				for i, err := range resultErrs {
					b := currentBlockNum + i
					if err != nil {
						errReached = true
					} else {
						finishedBlockNums[b] = true
						if !errReached {
							blockNum = b + 1
						}
					}
				}
				for k, _ := range finishedBlockNums {
					if k <= blockNum {
						delete(finishedBlockNums, k)
					}
				}
				time.Sleep(interval)
			}
		}
	}()
}

func (w *BlockFileWatcher) Close() {
	w.startLock.Lock()
	defer w.startLock.Unlock()
	if w.chDone != nil {
		close(w.chDone)
		w.chDone = nil
	}
}
