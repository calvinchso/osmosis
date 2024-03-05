package keepers

import (
	sdk "github.com/cosmos/cosmos-sdk/types"

	poolmanagertypes "github.com/osmosis-labs/osmosis/v23/x/poolmanager/types"
)

type GAMMKeeper interface {
	GetPools(ctx sdk.Context) ([]poolmanagertypes.PoolI, error)
}

type BankKeeper interface {
	GetAllBalances(ctx sdk.Context, addr sdk.AccAddress) sdk.Coins
}

type ConcentratedKeeper interface {
	GetPools(ctx sdk.Context) ([]poolmanagertypes.PoolI, error)
}
