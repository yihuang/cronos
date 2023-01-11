# Alternative IAVL Implementation

## The Journey

It started for an use case of verifying the state change sets, we need to replay the change sets to rebuild IAVL tree and check the final IAVL root hash, compare the root hash with the on-chain hash to verify the integrity of the change sets.

The first implementation keeps the whole IAVL tree in memory, mutate nodes in-place, and don't update hashes for the intermediate versions, and one insight from the test run is it runs surprisingly fast. For the distribution store in our testnet, it can process from genesis to block `6698242` in 2 minutes, which is around `55818` blocks per second.

To support incremental replay, we further designed an IAVL snapshot format that's stored on disk, while supporting random access with mmap, which solves the memory usage issue, and reduce the time of replaying.

## New Design

So the new idea is we can put the snapshot and change sets together, the change sets is the write-ahead-log for the IAVL tree.

It also integrates well with versiondb, because versiondb can also be derived from change sets to provide query service. IAVL tree is only used for consensus state machine and merkle proof generations.

### Advantages

- Better write amplification, we only need to write the change sets in real time which is much more compact than IAVL nodes, IAVL snapshot can be created in much lower frequency.
- Better read amplification, the IAVL snapshot is a plain file, the nodes are referenced with offset, the read amplification is simply 1.
- Better space amplification, the archived change sets are much more compact than current IAVL tree, in our test case, the ratio could be as large as 1:100. We don't need to keep too old IAVL snapshots, because versiondb will handle the historical key-value queries, IAVL tree only takes care of merkle proof generations for blocks within an unbonding period. In very rare cases that do need IAVL tree of very old version, you can always replay the change sets from the genesis.

## File Formats

> NOTICE: the integers are always encoded with little endianness.

### Change Set File

```
version: int64
size: int64
payload: protobuf encoded ChangeSet message

repeat with next version
```

- Change set files can be splited with certain block ranges for incremental backup and restoration.

- Historical files can be compressed with zlib, because it don't need to support random access.

### IAVL Snapshot

IAVL snapshot is composed by four files:

- `metadata`, two integers:

  ```
  version: int64
  root node index: int64
  ```

- `nodes`, array of fixed size(64bytes) nodes, the node format is like this:

  ```
  height  : int8          // padded to 4bytes
  version : int32
  size    : int64
  key     : int64         // offset in keys file
  left    : int32         // inner node only
  right   : int32         // inner node only
  value   : int64 offset  // offset in values file, leaf node only
  hash    : [32]byte
  ```
  The node has fixed length, can be indexed directly. The nodes reference each other with the index, nodes are written in post-order, so the root node is always placed at the end.

  Some integers are using `int32`, should be enough in forseeable future, but could be changed to `int64` to be safer.

  The implementation will read the mmap-ed content in a zero-copy way, don't use extra node cache, just rely on OS page cache.

- `keys`, sequence of length prefixed node keys, deduplicated.

  ```
  size: int16
  payload
  ...
  ```

  Key size is encoded in `int16`, so maximum key length supported is `65536`. 

- `values`, sequence of length prefixed leaf node values.

  ```
  size: int32
  payload
  ...
  ```

  Value size is encoded in `int32`, so maximum key length supported is `2**32`, around 4G.

### VersionDB

VersionDB is to support query and iterating historical versions of key-values pairs, currently implemented with rocksdb's experimental user-defined timestamp feature, support query and iterate key-value pairs by version, it's an alternative way to support grpc query service, and much more compact than IAVL tree, similar in size with the compressed change set files.