package client

import (
	"fmt"
	"io"
	"os"

	"github.com/cosmos/gogoproto/jsonpb"
	"github.com/spf13/cobra"

	"github.com/crypto-org-chain/cronos/versiondb"
)

func PrintPlainFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print-plain [plain-file]",
		Short: "Pretty-print content of plain changeset file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			noParseChangeset, err := cmd.Flags().GetBool(flagNoParseChangeset)
			if err != nil {
				return err
			}

			startVersion, err := cmd.Flags().GetInt64(flagStartVersion)
			if err != nil {
				return err
			}
			endVersion, err := cmd.Flags().GetInt64(flagEndVersion)
			if err != nil {
				return err
			}

			marshaler := jsonpb.Marshaler{}
			return withPlainInput(args[0], func(reader io.Reader) error {
				offset, err := readPlainFile(reader, func(version int64, changeSet *versiondb.ChangeSet) (bool, error) {
					if version < startVersion {
						return true, nil
					}
					if endVersion > 0 && version >= endVersion {
						return false, nil
					}
					fmt.Printf("version: %d\n", version)
					for _, pair := range changeSet.Pairs {
						js, err := marshaler.MarshalToString(pair)
						if err != nil {
							return false, err
						}
						fmt.Println(js)
					}
					return true, nil
				}, !noParseChangeset)
				if err == io.ErrUnexpectedEOF {
					// incomplete end of file, we'll output a warning and process the completed versions.
					fmt.Fprintf(os.Stderr, "file incomplete, the completed versions are processed, the last completed file offset: %d\n", offset)
				} else if err != nil {
					return err
				}
				return nil
			})
		},
	}
	cmd.Flags().Bool(flagNoParseChangeset, false, "if parse and output the change set content, otherwise only version numbers are outputted")
	cmd.Flags().Int64(flagStartVersion, 0, "Start of the version range to print")
	cmd.Flags().Int64(flagEndVersion, 0, "End(exclusive) of the version range to print, 0 means no end")
	return cmd
}
