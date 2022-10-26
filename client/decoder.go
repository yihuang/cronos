package client

import (
	"github.com/cosmos/cosmos-sdk/store/types"
	"github.com/gogo/protobuf/proto"
)

func DecodeData(data []byte) (pairs []types.StoreKVPair, err error) {
	offset := 0
	for offset < len(data) {
		size, n := proto.DecodeVarint(data[offset:])
		offset += n
		pair := new(types.StoreKVPair)
		if err := proto.Unmarshal(data[offset:offset+int(size)], pair); err != nil {
			return nil, err
		}
		pairs = append(pairs, *pair)
		offset += int(size)
	}
	return
}
