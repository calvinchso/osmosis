package poolsingester

import (
	"encoding/json"
	"fmt"
	"math"

	cosmosmath "cosmossdk.io/math"

	"github.com/cometbft/cometbft/libs/log"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/osmosis-labs/osmosis/osmomath"
	"github.com/osmosis-labs/osmosis/v23/ingest"
	externalservice "github.com/osmosis-labs/osmosis/v23/ingest/pools/externalservice"
	kafkaclient "github.com/osmosis-labs/osmosis/v23/ingest/pools/kafkaclient"
	keepers "github.com/osmosis-labs/osmosis/v23/ingest/pools/keepers"
	pooldata "github.com/osmosis-labs/osmosis/v23/ingest/pools/pooldata"
	poolmanagertypes "github.com/osmosis-labs/osmosis/v23/x/poolmanager/types"
)

const ingesterName = "pools-ingester"

type PoolsIngester struct {
	concentratedKeeper keepers.ConcentratedKeeper
	bankKeeper         keepers.BankKeeper
	logger             log.Logger
	osmoUSDPrice       osmomath.BigDec
	baseDenomsMapping  map[string][]externalservice.DenomUnit
	kafkaClient        kafkaclient.KafkaClient
}

func NewPoolsIngester(concentratedKeeper keepers.ConcentratedKeeper, bankKeeper keepers.BankKeeper, baseDenomsMapping map[string][]externalservice.DenomUnit, kafkaClient kafkaclient.KafkaClient) (ingest.Ingester, error) {
	return &PoolsIngester{
		concentratedKeeper: concentratedKeeper,
		bankKeeper:         bankKeeper,
		baseDenomsMapping:  baseDenomsMapping,
		kafkaClient:        kafkaClient,
	}, nil
}

// GetName implements ingest.Ingester.
func (i PoolsIngester) GetName() string {
	return ingesterName
}

func (i PoolsIngester) Logger(ctx sdk.Context) log.Logger {
	return ctx.Logger().With("module", ingesterName)
}

func (i PoolsIngester) getExponent(denom string) (int, error) {
	denomUnits, found := i.baseDenomsMapping[denom]
	if !found {
		return 0, fmt.Errorf("%s exponent not found", denom)
	}
	return denomUnits[1].Exponent, nil
}

// Given the PoolI interface, perform the liquidity (USD) calculation for each targeted pools as required
// Data transformation / business logic can be found here: https://hackmd.io/@osmosis/Bkin8VZhp#Data-Transformation
// Need extra precaution in handling & maintainging precision, i.e. avoid loss of precision during math operations, thus using cosmosmath.LegacyDec and its methods
func (i *PoolsIngester) calculateUSDLiquidity(ctx sdk.Context, pool poolmanagertypes.PoolI) (*pooldata.PoolData, error) {
	poolId := pool.GetId()
	poolType := pool.GetType().String()
	poolDenoms := pool.GetPoolDenoms(ctx)
	switch poolId {
	case 1263: // 1263 (OSMO/USDC)
		// We need to keep the spot price of OSMO/USDC for calculating other pool's liquidity in USD
		i.logger.Debug(fmt.Sprintf("Pool info - id: %d, address: %d, type: %s, denom1: %s, denom2: %s", pool.GetId(), pool.GetAddress(), pool.GetType(), poolDenoms[0], poolDenoms[1]))
		i.osmoUSDPrice, _ = pool.SpotPrice(ctx, poolDenoms[1], poolDenoms[0])
		i.logger.Debug(fmt.Sprintf("spot price: %f per %s", i.osmoUSDPrice, poolDenoms[0]))
		poolBalances := i.bankKeeper.GetAllBalances(ctx, pool.GetAddress())
		poolData := pooldata.PoolData{ID: poolId, Type: poolType}
		poolUSDBalance := cosmosmath.LegacyNewDec(0)
		for _, poolBalance := range poolBalances {
			denom := poolBalance.Denom
			exponent, err := i.getExponent(denom)
			if err != nil {
				return nil, err
			}
			balance := poolBalance.Amount.ToLegacyDec().Quo(cosmosmath.LegacyNewDec(int64(math.Pow(10, float64(exponent)))))
			if denom == "uosmo" {
				balanceUSDValue := i.osmoUSDPrice.Dec().Mul(balance)
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balanceUSDValue))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			} else {
				// USDC
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balance))
				poolUSDBalance = poolUSDBalance.Add(balance)
			}
		}
		poolData.USDLiquidity = poolUSDBalance.String()
		return &poolData, nil
	case 1281: // 1281 (ETH/OSMO)
		// Special treatment for 1281 - for some reason, pool 1281 spot price needs to be multiply by 10**12
		i.logger.Debug(fmt.Sprintf("Pool info - id: %d, address: %d, type: %s, denom1: %s, denom2: %s", pool.GetId(), pool.GetAddress(), pool.GetType(), poolDenoms[0], poolDenoms[1]))
		spotPrice, _ := pool.SpotPrice(ctx, poolDenoms[1], poolDenoms[0])
		spotPrice = spotPrice.Mul(osmomath.NewBigDec(int64(math.Pow(10, 12))))
		i.logger.Debug(fmt.Sprintf("spot price: %f per %s", spotPrice, poolDenoms[0]))
		poolBalances := i.bankKeeper.GetAllBalances(ctx, pool.GetAddress())
		poolData := pooldata.PoolData{ID: poolId, Type: poolType}
		poolUSDBalance := cosmosmath.LegacyNewDec(0)
		for _, poolBalance := range poolBalances {
			denom := poolBalance.Denom
			exponent, err := i.getExponent(denom)
			if err != nil {
				return nil, err
			}
			balance := poolBalance.Amount.ToLegacyDec().Quo(cosmosmath.LegacyNewDec(int64(math.Pow(10, float64(exponent)))))
			if denom == "uosmo" {
				balanceUSDValue := i.osmoUSDPrice.Dec().Mul(balance)
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s (%s)", balanceUSDValue, denom))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			} else {
				// Convert balance to no. of OSMO tokens
				balance = balance.Mul(spotPrice.Dec())
				balanceUSDValue := balance.Mul(i.osmoUSDPrice.Dec())
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s (%s)", balanceUSDValue, denom))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			}
		}
		poolData.USDLiquidity = poolUSDBalance.String()
		return &poolData, nil
	case 1400, 1265: //  1400 (OSMO/ATOM) 1265 (OSMO/ATOM)
		// Other pools' liquidities are handled below
		i.logger.Debug(fmt.Sprintf("Pool info - id: %d, address: %d, type: %s, denom1: %s, denom2: %s", pool.GetId(), pool.GetAddress(), pool.GetType(), poolDenoms[0], poolDenoms[1]))
		spotPrice, _ := pool.SpotPrice(ctx, poolDenoms[1], poolDenoms[0])
		i.logger.Debug(fmt.Sprintf("spot price: %f per %s", spotPrice, poolDenoms[0]))
		poolBalances := i.bankKeeper.GetAllBalances(ctx, pool.GetAddress())
		poolData := pooldata.PoolData{ID: poolId, Type: poolType}
		poolUSDBalance := cosmosmath.LegacyNewDec(0)
		for _, poolBalance := range poolBalances {
			denom := poolBalance.Denom
			exponent, err := i.getExponent(denom)
			if err != nil {
				return nil, err
			}
			balance := poolBalance.Amount.ToLegacyDec().Quo(cosmosmath.LegacyNewDec(int64(math.Pow(10, float64(exponent)))))
			if denom == "uosmo" {
				balanceUSDValue := i.osmoUSDPrice.Dec().Mul(balance)
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balanceUSDValue))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			} else {
				// Convert balance to no. of OSMO tokens
				balance = balance.Mul(cosmosmath.LegacyNewDec(1).Quo(spotPrice.Dec()))
				balanceUSDValue := balance.Mul(i.osmoUSDPrice.Dec())
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balanceUSDValue))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			}
		}
		poolData.USDLiquidity = poolUSDBalance.String()
		return &poolData, nil
	case 1335: // 1335 (milkTIA/TIA)
		// Special treatment for milkTIA & TIA as their spot prices have to be fetched thru some REST call
		i.logger.Debug(fmt.Sprintf("Pool info - id: %d, address: %d, type: %s, denom1: %s, denom2: %s", pool.GetId(), pool.GetAddress(), pool.GetType(), poolDenoms[0], poolDenoms[1]))
		sqsResponse, err := externalservice.FetchSqSData()
		if err != nil {
			return nil, err
		}
		i.logger.Debug(fmt.Sprintf("mileTia price in USD: %f, tia price in USD: %f", sqsResponse.MilkTiaPriceInUSD, sqsResponse.TiaPriceInUSD))
		poolBalances := i.bankKeeper.GetAllBalances(ctx, pool.GetAddress())
		poolData := pooldata.PoolData{ID: poolId, Type: poolType}
		poolUSDBalance := cosmosmath.LegacyNewDec(0)
		for _, poolBalance := range poolBalances {
			denom := poolBalance.Denom
			exponent, err := i.getExponent(denom)
			if err != nil {
				return nil, err
			}
			balance := poolBalance.Amount.ToLegacyDec().Quo(cosmosmath.LegacyNewDec(int64(math.Pow(10, float64(exponent)))))
			if denom == "factory/osmo1f5vfcph2dvfeqcqkhetwv75fda69z7e5c2dldm3kvgj23crkv6wqcn47a0/umilkTIA" {
				// milkTia
				tokenPrice, err := cosmosmath.LegacyNewDecFromStr(fmt.Sprintf("%f", sqsResponse.MilkTiaPriceInUSD))
				if err != nil {
					return nil, err
				}
				balanceUSDValue := tokenPrice.Mul(balance)
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balanceUSDValue))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			} else if denom == "ibc/D79E7D83AB399BFFF93433E54FAA480C191248FC556924A2A8351AE2638B3877" {
				// Tia
				tokenPrice, err := cosmosmath.LegacyNewDecFromStr(fmt.Sprintf("%f", sqsResponse.TiaPriceInUSD))
				if err != nil {
					return nil, err
				}
				balanceUSDValue := tokenPrice.Mul(balance)
				i.logger.Debug(fmt.Sprintf("Pool value in USD %s", balanceUSDValue))
				poolUSDBalance = poolUSDBalance.Add(balanceUSDValue)
			}
		}
		poolData.USDLiquidity = poolUSDBalance.String()
		return &poolData, nil
	}
	return nil, nil
}

// ProcessBlock implements ingest.Ingester.
func (i PoolsIngester) ProcessBlock(ctx sdk.Context) error {
	// Setting up the logger
	i.logger = i.Logger(ctx)

	// Getting all pool data
	pools, err := i.concentratedKeeper.GetPools(ctx)
	if err != nil {
		return err
	}

	var poolLiquidities []pooldata.PoolData
	// Iterate the pool and obtain the pool data struct
	for _, pool := range pools {
		poolData, _ := i.calculateUSDLiquidity(ctx, pool)
		if poolData != nil {
			poolLiquidities = append(poolLiquidities, *poolData)
		}
	}

	// Sending the data to Kafka
	i.logger.Info(fmt.Sprintf("Pool liquidities data to be sent: %v", poolLiquidities))
	eventBytes, err := json.Marshal(poolLiquidities)
	if err != nil {
		return err
	}
	i.kafkaClient.Send(eventBytes)
	i.logger.Info(fmt.Sprintf("Pool data sent to Kafka"))

	return nil

}
