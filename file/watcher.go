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
	FilePath string
}

type BlockFileWatcher struct {
	concurrency int
	getFilePath func(blockNum int) string
	downloader  fileDownloader
	chData      chan *BlockData
	chError     chan error
	chDone      chan bool
	startLock   *sync.Mutex
	directory   string
}

func NewBlockFileWatcher(
	concurrency int,
	getFilePath func(blockNum int) string,
	isLocal bool,
	directory string,
) *BlockFileWatcher {
	w := &BlockFileWatcher{
		concurrency: concurrency,
		getFilePath: getFilePath,
		chData:      make(chan *BlockData),
		chError:     make(chan error),
		startLock:   new(sync.Mutex),
		directory:   directory,
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

func (w *BlockFileWatcher) fetch(blockNum int) (string, error) {
	path := w.getFilePath(blockNum)
	data, err := w.downloader.GetData(path)
	fmt.Printf("mm-path: %+v, %+v, %+v\n", path, len(data), err)
	if err != nil {
		if err != errNotExist {
			// avoid blocked by error when not subscribe
			select {
			case w.chError <- err:
			default:
			}
		}
		return "", err
	}
	w.chData <- &BlockData{
		BlockNum: blockNum,
		FilePath: path,
	}
	file := GetLocalDataFileName(w.directory, blockNum)
	fmt.Printf("mm-file: %+v\n", file)
	err = os.WriteFile(file, data, 0644)
	if err != nil {
		fmt.Printf("mm-writeErr: %+v\n", err)
		return "", err
	}
	return file, nil
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
				resultFiles := make([]string, w.concurrency)
				for i := 1; i <= w.concurrency; i++ {
					fmt.Println("mm-start: ", i)
					if !finishedBlockNums[blockNum+i] {
						wg.Add(1)
						go func(i int) {
							f, err := w.fetch(blockNum + i)
							if err == nil {
								resultFiles[i-1] = f
							}
							wg.Done()
						}(i)
					}
				}
				wg.Wait()
				errReached := false
				finishedBlockNums = make(map[int]bool)
				currentBlockNum := blockNum
				for i, f := range resultFiles {
					b := currentBlockNum + i + 1
					if f == "" {
						errReached = true
					} else {
						finishedBlockNums[b] = true
						if !errReached {
							blockNum = b
						}
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
