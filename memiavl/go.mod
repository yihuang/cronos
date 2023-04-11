module github.com/crypto-org-chain/cronos/memiavl

go 1.20

require (
	github.com/cosmos/cosmos-sdk v0.46.11
	github.com/cosmos/gogoproto v1.4.7
	github.com/cosmos/iavl v0.19.6-0.20230410112350-e1e212a3a3aa
	github.com/gogo/protobuf v1.3.2
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/ledgerwatch/erigon-lib v0.0.0-20230210071639-db0e7ed11263
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.1
	github.com/tendermint/tm-db v0.6.7
	github.com/tidwall/btree v1.5.0
	github.com/tidwall/wal v1.1.7
	golang.org/x/exp v0.0.0-20230206171751-46f607a40771
	golang.org/x/sync v0.1.0
)

require (
	cosmossdk.io/errors v1.0.0-beta.7 // indirect
	github.com/VictoriaMetrics/metrics v1.23.1 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.3.2 // indirect
	github.com/c2h5oh/datasize v0.0.0-20220606134207-859f65c6625b // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/confio/ics23/go v0.9.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/dgraph-io/badger/v2 v2.2007.4 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/jmhodges/levigo v1.0.0 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/ledgerwatch/log/v3 v3.7.0 // indirect
	github.com/linxGnu/grocksdb v1.7.10 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7 // indirect
	github.com/tendermint/go-amino v0.16.0 // indirect
	github.com/tendermint/tendermint v0.34.27 // indirect
	github.com/tidwall/gjson v1.10.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tidwall/tinylru v1.1.0 // indirect
	github.com/torquem-ch/mdbx-go v0.27.5 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20230202175211-008b39050e57 // indirect
	google.golang.org/grpc v1.53.0 // indirect
	google.golang.org/protobuf v1.29.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace (
	github.com/gogo/protobuf => github.com/regen-network/protobuf v1.3.3-alpha.regen.1
	// use cometbft
	github.com/tendermint/tendermint => github.com/cometbft/cometbft v0.34.27
	// https://github.com/crypto-org-chain/tm-db/tree/release/v0.6.x
	github.com/tendermint/tm-db => github.com/crypto-org-chain/tm-db v0.6.8-0.20230118040049-14dc6b00a5b3
	github.com/tidwall/wal => github.com/yihuang/wal v1.1.8-0.20230412034935-cac7e793fa45
)
