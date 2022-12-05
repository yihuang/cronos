package client

import (
	"fmt"

	"github.com/cosmos/cosmos-sdk/store/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gogo/protobuf/proto"
)

func DecodeData(data []byte) (pairs []types.StoreKVPair, err error) {
	const prefixLen = 8
	offset := prefixLen
	dataSize := sdk.BigEndianToUint64(data[:offset])
	size := len(data)
	if int(dataSize)+prefixLen != size {
		return nil, fmt.Errorf("incomplete file: %v vs %v", dataSize, size)
	}
	for offset < size {
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
