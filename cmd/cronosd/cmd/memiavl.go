package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/cosmos/iavl"
	"github.com/crypto-org-chain/cronos/memiavl"
	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/spf13/cobra"
)

const (
	flagSnapshotVersion = "snapshot-version"
	flagOutputDir       = "output-dir"
)

func ImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import [input_dir]",
		Short: "import snapshot to convert state-sync snapshot to memiavl snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			outputDir, err := cmd.Flags().GetString(flagOutputDir)
			if err != nil {
				return err
			}
			if outputDir == "" {
				outputDir = fmt.Sprintf("%s-output", dir)
			} else if dir == outputDir {
				return errors.New("output same as import dir")
			}
			version, err := cmd.Flags().GetInt64(flagSnapshotVersion)
			if err != nil {
				return err
			}
			keys, _, _ := app.StoreKeys()
			for name := range keys {
				moduleDir := fmt.Sprintf("%s/%s", dir, name)
				// read file to snapshot
				snapshot, err := memiavl.OpenSnapshot(moduleDir)
				if err != nil {
					return err
				}
				ch := make(chan *iavl.ExportNode)
				chanErr := make(chan error)
				go func() {
					defer close(ch)
					exporter := snapshot.Export()
					for {
						node, err := exporter.Next()
						if err == iavl.ExportDone {
							break
						}
						if err != nil {
							chanErr <- err
							return
						}
						ch <- node
					}
				}()
				go func() {
					moduleDir = fmt.Sprintf("%s/%s", outputDir, name)
					err := os.MkdirAll(moduleDir, os.ModePerm)
					if err == nil {
						err = memiavl.Import(moduleDir, version, ch, true)
					}
					if err != nil {
						panic(err)
					}
					chanErr <- err
				}()
				err = <-chanErr
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64(flagSnapshotVersion, 0, "Snapshot version")
	cmd.Flags().String(flagOutputDir, "", "Output directory")
	return cmd
}
