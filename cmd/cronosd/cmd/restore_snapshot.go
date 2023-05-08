package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	servertypes "github.com/cosmos/cosmos-sdk/server/types"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
)

func RestoreSnapshotCmd(appCreator servertypes.AppCreator, defaultNodeHome string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore-snapshot path-to-snapshot",
		Short: "Restore app state from snapshot saved by the option `store.save-snapshot-dir`",
		Long:  "Restore app state from snapshot saved by the option `store.save-snapshot-dir`",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := server.GetServerContextFromCmd(cmd)

			dir := args[0]
			bz, err := os.ReadFile(filepath.Join(dir, "snapshot"))
			if err != nil {
				return fmt.Errorf("read snapshot failed: %v", err)
			}

			var request abci.RequestOfferSnapshot
			if err := request.Unmarshal(bz); err != nil {
				return fmt.Errorf("unmarshal snapshot failed: %v", err)
			}

			home := ctx.Config.RootDir
			db, err := opendb.OpenDB(home, server.GetAppDBBackend(ctx.Viper))
			if err != nil {
				return fmt.Errorf("open db failed: %v", err)
			}
			logger := log.NewTMLogger(log.NewSyncWriter(os.Stdout))
			app := appCreator(logger, db, nil, ctx.Viper)

			rsp := app.OfferSnapshot(request)
			if rsp.Result != abci.ResponseOfferSnapshot_ACCEPT {
				return fmt.Errorf("offer snapshot failed: %v", rsp.Result)
			}

			snapshot := request.Snapshot
			var i uint32
			for ; i < snapshot.Chunks; i++ {
				chunkPath := filepath.Join(dir, strconv.FormatUint(uint64(i), 10))
				bz, err := os.ReadFile(chunkPath)
				if err != nil {
					fmt.Errorf("read chunk %d failed: %v", i, err)
				}
				res := app.ApplySnapshotChunk(abci.RequestApplySnapshotChunk{
					Index: i,
					Chunk: bz,
				})
				if res.Result != abci.ResponseApplySnapshotChunk_ACCEPT {
					return fmt.Errorf("apply chunk %d failed: %v", i, res.Result)
				}
			}

			lastCommitID := app.CommitMultiStore().LastCommitID()
			if uint64(lastCommitID.Version) != snapshot.Height {
				return fmt.Errorf("commit height %d not match snapshot height %d", lastCommitID.Version, snapshot.Height)
			}

			if !bytes.Equal(lastCommitID.Hash, request.AppHash) {
				return fmt.Errorf("commit app hash %s not match snapshot request %s, height: %d", hex.EncodeToString(lastCommitID.Hash), hex.EncodeToString(snapshot.Hash), snapshot.Height)
			}

			fmt.Printf("restore snapshot %d successfully\n", snapshot.Height)
			return nil
		},
	}
	cmd.Flags().String(flags.FlagHome, defaultNodeHome, "The application home directory")
	return cmd
}
