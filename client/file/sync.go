package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/crypto-org-chain/cronos/client"
	"github.com/crypto-org-chain/cronos/versiondb/tmdb"
	tmjson "github.com/tendermint/tendermint/libs/json"
	tmtypes "github.com/tendermint/tendermint/types"
)

// Simplify block height for header
type Header struct {
	Height int64 `json:"height,omitempty"`
}

type Block struct {
	Header Header `json:"header"`
}

type GetLatestBlockResponse struct {
	Block *Block `json:"block,omitempty"`
}

func Sync(versionDB *tmdb.Store, remoteGrpcUrl, remoteUrl, remoteWsUrl, rootDir string, isLocal bool, concurrency int) {

	const defaultMaxRetry = 50
	latestVersion, err := versionDB.GetLatestVersion()
	fmt.Printf("mm-latestVersion: %+v\n", latestVersion)
	if err != nil {
		panic(err)
	}
	startBlockNum := latestVersion
	if startBlockNum < 0 {
		startBlockNum = 0
	}
	nextBlockNum := int(startBlockNum) + 1
	maxBlockNum := -1
	for i := 0; i < defaultMaxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}
		resp, err := http.Get(fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/latest", remoteGrpcUrl))
		if err != nil {
			fmt.Printf("error making http request: %s\n", err)
			continue
		}

		var bz []byte
		if result := func() bool {
			defer resp.Body.Close()
			bz, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return false
			}
			if resp.StatusCode != http.StatusOK {
				return false
			}
			return true
		}(); !result {
			continue
		}

		res := new(GetLatestBlockResponse)
		err = tmjson.Unmarshal(bz, res)
		if err != nil {
			fmt.Printf("mm-read-res-err: %+v\n", err)
			continue
		}
		maxBlockNum = int(res.Block.Header.Height)
		fmt.Printf("mm-maxBlockNum: %d\n", maxBlockNum)
		break
	}
	if maxBlockNum < 0 {
		panic(fmt.Sprintf("max retries %d reached", defaultMaxRetry))
	}

	interval := time.Second
	directory := filepath.Join(rootDir, "data", "file_streamer")
	// streamer write the file blk by blk with concurrency 1
	streamer := NewBlockFileWatcher(1, maxBlockNum, func(blockNum int) string {
		return GetLocalDataFileName(directory, blockNum)
	}, nil, true)
	streamer.Start(nextBlockNum, interval)
	go func() {
		chData, chErr := streamer.SubscribeData(), streamer.SubscribeError()
		for {
			select {
			case data := <-chData:
				pairs, err := client.DecodeData(data.Data)
				fmt.Printf("mm-pairs: %+v, %+v\n", len(pairs), err)
				if err == nil {
					if err = versionDB.PutAtVersion(int64(data.BlockNum), pairs); err != nil {
						fmt.Println("mm-put-at-version-panic")
						panic(err)
					}
				}
			case err := <-chErr:
				// fail read
				fmt.Println("mm-fail-read-panic")
				panic(err)
			}
		}
	}()

	synchronizer := NewBlockFileWatcher(
		concurrency,
		maxBlockNum,
		func(blockNum int) string {
			return fmt.Sprintf("%s/%s", remoteUrl, DataFileName(blockNum))
		},
		func(blockNum int) bool {
			path := GetLocalDataFileName(directory, blockNum)
			f, err := os.Open(path)
			if err == nil {
				defer func() {
					_ = f.Close()
				}()
				// valid 1st 8 bytes for downloaded file
				var bytes [8]byte
				if _, err = io.ReadFull(f, bytes[:]); err == nil {
					size := binary.BigEndian.Uint64(bytes[:])
					if info, err := f.Stat(); err == nil && size+uint64(8) == uint64(info.Size()) {
						return false
					}
				}
			}
			return true
		},
		isLocal,
	)
	synchronizer.Start(nextBlockNum, interval)
	go func() {
		// max retry for temporary io error
		maxRetry := concurrency * 2
		retry := 0
		chData, chErr := synchronizer.SubscribeData(), synchronizer.SubscribeError()
		for {
			select {
			case data := <-chData:
				file := GetLocalDataFileName(directory, data.BlockNum)
				fmt.Printf("mm-data.BlockNum: %+v\n", data.BlockNum)
				if err := os.WriteFile(file, data.Data, 0600); err != nil {
					fmt.Println("mm-WriteFile-panic")
					panic(err)
				}
				retry = 0
				fmt.Println("mm-reset-retry")
				if data.BlockNum > maxBlockNum {
					streamer.SetMaxBlockNum(data.BlockNum)
				}
			case err := <-chErr:
				retry++
				fmt.Println("mm-retry", retry)
				if retry == maxRetry {
					// data corrupt
					fmt.Println("mm-data-corrupt-panic")
					panic(err)
				}
			}
		}
	}()

	go func() {
		for i := 0; i < defaultMaxRetry; i++ {
			if i > 0 {
				time.Sleep(time.Second)
			}
			wsClient := client.NewWebsocketClient(remoteWsUrl)
			chResult, err := wsClient.Subscribe()
			if err != nil {
				fmt.Printf("mm-subscribed[%+v]: %+v\n", i, err)
				continue
			}
			fmt.Println("subscribing")
			err = wsClient.Send("subscribe", []string{
				"tm.event='NewBlockHeader'",
			})
			if err != nil {
				fmt.Printf("mm-subscribed: %+v\n", err)
				continue
			}
			i = 0
			fmt.Println("subscribed ws")
			for res := range chResult {
				if res == nil || res.Data == nil {
					continue
				}
				data, ok := res.Data.(tmtypes.EventDataNewBlockHeader)
				if !ok {
					continue
				}
				blockNum := int(data.Header.Height)
				fmt.Printf("mm-set-max-blk: %+v\n", blockNum)
				synchronizer.SetMaxBlockNum(blockNum)
				streamer.SetMaxBlockNum(blockNum)
			}
		}
		panic(fmt.Sprintf("max retries %d reached", defaultMaxRetry))
	}()
}
