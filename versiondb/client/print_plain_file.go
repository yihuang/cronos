package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/cosmos/iavl"
	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
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

			marshaler := jsonpb.Marshaler{}
			return withPlainInput(args[0], func(reader io.Reader) error {
				offset, err := readPlainFile(reader, func(version int64, changeSet *iavl.ChangeSet) error {
					fmt.Printf("version: %d\n", version)
					for _, pair := range changeSet.Pairs {
						js, err := marshaler.MarshalToString(pair)
						if err != nil {
							return err
						}
						fmt.Println(js)
					}
					return nil
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
	return cmd
}

func readPlainFile(input io.Reader, fn func(version int64, changeSet *iavl.ChangeSet) error, parseChangeset bool) (int, error) {
	var (
		err             error
		versionHeader   [16]byte
		lastValidOffset int
	)

	lastValidOffset = 0
	for {
		if _, err = io.ReadFull(input, versionHeader[:]); err != nil {
			break
		}
		version := binary.LittleEndian.Uint64(versionHeader[:8])
		size := int(binary.LittleEndian.Uint64(versionHeader[8:16]))
		var changeSet iavl.ChangeSet
		if size > 0 {
			if parseChangeset {
				buf := make([]byte, size)
				if _, err = io.ReadFull(input, buf[:]); err != nil {
					break
				}

				if err = proto.Unmarshal(buf[:], &changeSet); err != nil {
					return lastValidOffset, err
				}
			} else {
				if written, err := io.CopyN(ioutil.Discard, input, int64(size)); err != nil {
					if err == io.EOF && written < int64(size) {
						err = io.ErrUnexpectedEOF
					}
					break
				}
			}
		}

		if err = fn(int64(version), &changeSet); err != nil {
			return lastValidOffset, err
		}

		lastValidOffset += size + 16
	}

	if err != io.EOF {
		return lastValidOffset, err
	}

	return lastValidOffset, nil
}
