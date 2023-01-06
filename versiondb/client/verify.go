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
		Use:   "verify-plain [plain-file]",
		Short: "Verify change set file by rebuild iavl tree in memory and check root hash",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withPlainInput(args[0], func(reader io.Reader) error {
				var (
					err           error
					latestVersion int64
				)
				tree := NewEmptyTree(0)
				offset, err := readPlainFile(reader, func(version int64, changeSet *versiondb.ChangeSet) error {
					for _, pair := range changeSet.Pairs {
						if pair.Delete {
							tree.Remove(pair.Key)
						} else {
							tree.Set(pair.Key, pair.Value)
						}
					}

					// no need to update hashes for intermidiate versions.
					_, latestVersion, err = tree.SaveVersion(false)
					if err != nil {
						return err
					}
					return nil
				}, true)

				if err == io.ErrUnexpectedEOF {
					// incomplete end of file, we'll output a warning and process the completed versions.
					fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
				} else if err != nil {
					return err
				}

				rootHash, err := tree.RootHash()
				if err != nil {
					return err
				}
				fmt.Printf("%d %X\n", latestVersion, rootHash)

				return nil
			})
		},
	}
	return cmd
}
