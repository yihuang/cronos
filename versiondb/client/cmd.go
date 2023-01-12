package client

import (
	"fmt"

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

			ingestBehind, err := cmd.Flags().GetBool(flagIngestBehind)
			if err != nil {
				return err
			}

			db, cfHandle, err := tsrocksdb.OpenVersionDB(dbPath, true)
			if err != nil {
				return err
			}
			if len(args) > 1 {
				ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
				ingestOpts.SetMoveFiles(moveFiles)
				ingestOpts.SetIngestionBehind(ingestBehind)
				if err := db.IngestExternalFileCF(cfHandle, args[1:], ingestOpts); err != nil {
					return err
				}
				if !ingestBehind {
					db.CompactRangeCF(cfHandle, grocksdb.Range{})
				}
			}

			maxVersion, err := cmd.Flags().GetInt64(flagMaximumVersion)
			if err != nil {
				return err
			}
			if maxVersion > 0 {
				// update latest version
				store := tsrocksdb.NewStoreWithDB(db, cfHandle)
				latestVersion, err := store.GetLatestVersion()
				if err != nil {
					return err
				}
				if maxVersion > latestVersion {
					fmt.Println("update latest version to", latestVersion)
					if err := store.SetLatestVersion(latestVersion); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().Bool(flagMoveFiles, false, "move sst files instead of copy them")
	cmd.Flags().Int64(flagMaximumVersion, 0, "Specify the maximum version covered by the ingested files, if it's bigger than existing recorded latest version, will update it.")
	cmd.Flags().Bool(flagIngestBehind, false, "Set ingestion behind flag in rocksdb")
	return cmd
}
