package file

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
)

var errNotExist = errors.New("file not exist")

type fileDownloader interface {
	GetData(path string) ([]byte, error)
}

type localFileDownloader struct{}

func (d *localFileDownloader) GetData(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
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
}

type BlockFileWatcher struct {
	concurrency int
	getFilePath func(blockNum int) string
	downloader  fileDownloader
	chData      chan *BlockData
	chError     chan error
	chDone      chan bool
	startLock   *sync.Mutex
}

func NewBlockFileWatcher(
	concurrency int,
	getFilePath func(blockNum int) string,
	isLocal bool,
) *BlockFileWatcher {
	w := &BlockFileWatcher{
		concurrency: concurrency,
		getFilePath: getFilePath,
		chData:      make(chan *BlockData),
		chError:     make(chan error),
		startLock:   new(sync.Mutex),
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

func (w *BlockFileWatcher) fetch(blockNum int) error {
	path := w.getFilePath(blockNum)
	// TBC: skip if exist path to avoid dup download
	data, err := w.downloader.GetData(path)
	fmt.Printf("mm-fetch: %+v, %+v, %+v\n", blockNum, len(data), err)
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
	w.chData <- &BlockData{
		BlockNum: blockNum,
		Data:     data,
	}
	return nil
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
				resultErrs := make([]error, w.concurrency)
				for i := 0; i < w.concurrency; i++ {
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
				currentBlockNum := blockNum
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
				for k := range finishedBlockNums {
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
