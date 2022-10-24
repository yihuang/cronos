package client

import (
	"bytes"
	"context"
	"fmt"
	"time"

	tmjson "github.com/tendermint/tendermint/libs/json"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	types "github.com/tendermint/tendermint/rpc/jsonrpc/types"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type WebsocketClient struct {
	url    string
	wsconn *websocket.Conn
}

func NewWebsocketClient(url string) *WebsocketClient {
	return &WebsocketClient{url: url}
}

func (c *WebsocketClient) Subscribe() (<-chan *coretypes.ResultEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return nil, err
	}

	c.wsconn = conn
	conn.SetReadLimit(10240000)

	chResult := make(chan *coretypes.ResultEvent)
	go func() {
		defer close(chResult)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			_, r, err := c.wsconn.Reader(ctx)
			if err != nil {
				cancel()
				continue
			}
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(r)
			if err != nil {
				cancel()
				continue
			}
			cancel()

			bz := buf.Bytes()
			res := new(types.RPCResponse)
			err = tmjson.Unmarshal(bz, res)
			if err != nil {
				fmt.Printf("mm-read-res-err: %+v\n", err)
				// break trigger close channel while continue will ignore the invalid response
				break
			}
			ev := new(coretypes.ResultEvent)
			if err := tmjson.Unmarshal(res.Result, ev); err != nil {
				fmt.Printf("mm-read-ev-err: %+v\n", err)
				break
			}
			chResult <- ev
			time.Sleep(time.Second)
		}
	}()
	return chResult, nil
}

func (c *WebsocketClient) Send(
	method string,
	params []string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	return wsjson.Write(ctx, c.wsconn, map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
}

func (c *WebsocketClient) Close() {
	if c.wsconn != nil {
		c.wsconn.Close(websocket.StatusNormalClosure, "")
		c.wsconn = nil
	}
}
