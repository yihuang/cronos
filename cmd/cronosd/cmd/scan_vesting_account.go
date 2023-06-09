package cmd

import (
	"fmt"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/cosmos/cosmos-sdk/server/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	vestingtypes "github.com/cosmos/cosmos-sdk/x/auth/vesting/types"
	"github.com/crypto-org-chain/cronos/app"
	ethermintserver "github.com/evmos/ethermint/server"
	"github.com/spf13/cobra"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
)

func ScanInvalidVestingAccountCmd(appCreator types.AppCreator, openDB ethermintserver.DBOpener) *cobra.Command {
	return &cobra.Command{
		Use:   "scan_invalid_vesting_account",
		Short: "Scan invalid vesting account",
		RunE: func(cmd *cobra.Command, args []string) error {
			srvCtx := server.GetServerContextFromCmd(cmd)
			home := srvCtx.Viper.GetString(flags.FlagHome)

			db, err := openDB(home, server.GetAppDBBackend(srvCtx.Viper))
			if err != nil {
				return err
			}

			srvCtx.Viper.Set("iavl-lazy-loading", true)
			app := appCreator(srvCtx.Logger, db, nil, srvCtx.Viper).(*app.App)

			ctx := sdk.NewContext(app.CommitMultiStore(), tmproto.Header{}, false, app.Logger())
			app.AccountKeeper.IterateAccounts(ctx, func(account authtypes.AccountI) bool {
				if vestingAcct, ok := account.(*vestingtypes.PeriodicVestingAccount); ok {
					for _, period := range vestingAcct.VestingPeriods {
						for _, coin := range period.Amount {
							if !coin.Amount.IsPositive() {
								fmt.Println(account.GetAddress().String(), coin)
							}
						}
					}
				}
				return false
			})

			return nil
		},
	}
}
