package cmd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/cosmos/iavl"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	dbm "github.com/tendermint/tm-db"
	"google.golang.org/protobuf/proto"
)

const (
	flagStartVersion = "start-version"
	flagEndVersion   = "end-version"
	flagOutput       = "output"
)

var VersionDBMagic = []byte("VERDB000")

func DumpChangeSetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dump-changeset",
		Short: "Extract changeset from iavl versions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := server.GetServerContextFromCmd(cmd)
			if err := ctx.Viper.BindPFlags(cmd.Flags()); err != nil {
				return err
			}

			backendType := server.GetAppDBBackend(ctx.Viper)
			home := ctx.Viper.GetString(flags.FlagHome)
			dataDir := filepath.Join(home, "data")
			db, err := dbm.NewDB("application", backendType, dataDir)
			if err != nil {
				return err
			}

			prefix := []byte(fmt.Sprintf("s/k:%s/", args[0]))
			db = dbm.NewPrefixDB(db, prefix)
			cacheSize := cast.ToInt(ctx.Viper.Get("iavl-cache-size"))

			startVersion, err := cmd.Flags().GetInt(flagStartVersion)
			if err != nil {
				return err
			}
			endVersion, err := cmd.Flags().GetInt(flagEndVersion)
			if err != nil {
				return err
			}

			output, err := cmd.Flags().GetString(flagOutput)
			if err != nil {
				return err
			}

			var writer io.Writer
			if output == "-" {
				writer = os.Stdout
			} else {
				fp, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					return err
				}
				defer fp.Close()
				bufWriter := bufio.NewWriter(fp)
				defer bufWriter.Flush()

				writer = bufWriter
				if _, err = writer.Write(VersionDBMagic); err != nil {
					return err
				}
			}

			var sizeBuf [binary.MaxVarintLen64]byte
			tree := iavl.NewImmutableTree(db, cacheSize, true)
			tree.TraverseStateChanges(int64(startVersion), int64(endVersion), func(version int64, changeSet *iavl.ChangeSet) error {
				bz, err := proto.Marshal(changeSet)
				if err != nil {
					return err
				}

				n := binary.PutUvarint(sizeBuf[:], uint64(version))
				writer.Write(sizeBuf[:n])

				n = binary.PutUvarint(sizeBuf[:], uint64(len(bz)))
				writer.Write(sizeBuf[:n])

				writer.Write(bz)
				return nil
			})

			return nil
		},
	}
	cmd.Flags().Int(flagStartVersion, 1, "The start version")
	cmd.Flags().Int(flagEndVersion, 0, "The end version, exclusive")
	cmd.Flags().String(flagOutput, "-", "Output file, default to stdout")
	return cmd
}
