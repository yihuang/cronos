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
			ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
			ingestOpts.SetMoveFiles(moveFiles)
			return db.IngestExternalFileCF(cfHandle, args[1:], ingestOpts)
		},
	}
	cmd.Flags().Bool(flagMoveFiles, false, "move sst files instead of copy them")
	return cmd
}
