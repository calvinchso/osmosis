package poolsingester

import (
	"github.com/osmosis-labs/osmosis/v23/ingest"
	externalservice "github.com/osmosis-labs/osmosis/v23/ingest/pools/externalservice"
	kafkaclient "github.com/osmosis-labs/osmosis/v23/ingest/pools/kafkaclient"
	keepers "github.com/osmosis-labs/osmosis/v23/ingest/pools/keepers"
)

type Config struct {
	KafkaBrokers []string
	KafkaTopic   string
}

var DefaultConfig = Config{
	KafkaBrokers: []string{"localhost:9092"},
	KafkaTopic:   "pool-liquidity-events",
}

func (c Config) Initialize(concentratedKeeper keepers.ConcentratedKeeper, bankKeeper keepers.BankKeeper) (ingest.Ingester, error) {
	// Initialize a kafaka client
	kafkaClient := kafkaclient.NewKafkaClient(c.KafkaBrokers, c.KafkaTopic)

	// Fetching base and denom units mapping. Exponent data is part of the denom unit struct
	baseDenomsMapping, err := externalservice.FetchAssets()
	if err != nil {
		return nil, err
	}

	// Passing the kafaka client to the PoolsIngester initializer
	poolIngester, err := NewPoolsIngester(concentratedKeeper, bankKeeper, baseDenomsMapping, *kafkaClient)
	if err != nil {
		return nil, err
	}
	return poolIngester, nil
}
