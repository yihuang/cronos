package client

import (
	"fmt"
	"io"
	"os"

	"github.com/crypto-org-chain/cronos/versiondb"
	"github.com/spf13/cobra"
)

func VerifyPlainFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify-plain [plain-file] [plain-file] ...",
		Short: "Verify change set file by rebuild iavl tree in memory and check root hash, the plain files must include continuous blocks",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetVersion, err := cmd.Flags().GetInt64(flagTargetVersion)
			if err != nil {
				return err
			}

			tree := NewEmptyTree(0)
			for _, fileName := range args {
				if err := withPlainInput(fileName, func(reader io.Reader) error {
					var err error
					offset, err := readPlainFile(reader, func(version int64, changeSet *versiondb.ChangeSet) (bool, error) {
						for _, pair := range changeSet.Pairs {
							if pair.Delete {
								tree.Remove(pair.Key)
							} else {
								tree.Set(pair.Key, pair.Value)
							}
						}

						// no need to update hashes for intermidiate versions.
						_, _, err = tree.SaveVersion(false)
						if err != nil {
							return false, err
						}
						return targetVersion == 0 || tree.Version() < targetVersion, nil
					}, true)

					if err == io.ErrUnexpectedEOF {
						// incomplete end of file, we'll output a warning and process the completed versions.
						fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
					}

					return err
				}); err != nil {
					return err
				}

				if targetVersion > 0 && tree.Version() >= targetVersion {
					break
				}
			}

			rootHash, err := tree.RootHash()
			if err != nil {
				return err
			}
			fmt.Printf("%d %X\n", tree.Version(), rootHash)
			return nil
		},
	}
	cmd.Flags().Int64(flagTargetVersion, 0, "specify the target version, otherwise it'll exhaust the plain files")
	return cmd
}
