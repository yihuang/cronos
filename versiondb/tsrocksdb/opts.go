package tsrocksdb

import "github.com/linxGnu/grocksdb"

const VersionDBCFName = "versiondb"

// NewVersionDBOpts returns the options used for the versiondb column family
func NewVersionDBOpts() *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetComparator(CreateTSComparator())
	opts.SetTargetFileSizeMultiplier(2)

	// block based table options
	blkOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	blkOpts.SetBlockSize(32 * 1024)
	blkOpts.SetFilterPolicy(grocksdb.NewRibbonFilterPolicy(9.9))
	blkOpts.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	blkOpts.SetPartitionFilters(true)
	blkOpts.SetDataBlockIndexType(grocksdb.KDataBlockIndexTypeBinarySearchAndHash)
	opts.SetBlockBasedTableFactory(blkOpts)
	opts.SetOptimizeFiltersForHits(true)

	// compression
	opts.SetCompression(grocksdb.ZSTDCompression)
	compressOpts := grocksdb.NewDefaultCompressionOptions()
	compressOpts.MaxDictBytes = 112640 // 110k
	compressOpts.Level = 12
	opts.SetCompressionOptions(compressOpts)
	opts.SetCompressionOptionsZstdMaxTrainBytes(compressOpts.MaxDictBytes * 100)
	opts.SetCompressionOptionsZstdDictTrainer(true)
	return opts
}

func OpenVersionDB(dir string) (*grocksdb.DB, *grocksdb.ColumnFamilyHandle, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(
		opts, dir, []string{"default", VersionDBCFName},
		[]*grocksdb.Options{
			opts,
			NewVersionDBOpts(),
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return db, cfHandles[1], nil
}
