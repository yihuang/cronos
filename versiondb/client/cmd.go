package client

import (
	"github.com/linxGnu/grocksdb"
	"github.com/spf13/cobra"

	"github.com/crypto-org-chain/cronos/versiondb/tsrocksdb"
)

func ChangeSetGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "changeset",
		Short: "dump changeset from iavl versions or manage changeset files",
	}
	cmd.AddCommand(
		DumpFileChangeSetCmd(),
		PrintPlainFileCmd(),
		VerifyPlainFileCmd(),
		ConvertPlainToSSTTSCmd(),
		IngestSSTCmd(),
	)
	return cmd
}

func IngestSSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest-sst db-path file1.sst file2.sst ...",
		Short: "Ingest sst files into database",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := args[0]
			moveFiles, err := cmd.Flags().GetBool(flagMoveFiles)
			if err != nil {
				return err
			}
			db, cfHandle, err := tsrocksdb.OpenVersionDB(dbPath)
			if err != nil {
				return err
			}
			if len(args) > 1 {
				ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
				ingestOpts.SetMoveFiles(moveFiles)
				if err := db.IngestExternalFileCF(cfHandle, args[1:], ingestOpts); err != nil {
					return err
				}
			}

			latestVersion, err := cmd.Flags().GetInt64(flagSetLatestVersion)
			if err != nil {
				return err
			}
			if latestVersion > 0 {
				// update latest version
				store := tsrocksdb.NewStoreWithDB(db, cfHandle)
				store.SetLatestVersion(latestVersion)
			}
			return nil
		},
	}
	cmd.Flags().Bool(flagMoveFiles, false, "move sst files instead of copy them")
	cmd.Flags().Int64(flagSetLatestVersion, 0, "set the latest version to specified one")
	return cmd
}
