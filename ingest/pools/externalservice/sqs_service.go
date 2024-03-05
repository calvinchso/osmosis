package externalservice

import (
	"encoding/json"
	"io"
	"net/http"
)

type SqSResponse struct {
	MilkTiaPriceInUSD float32 `json:"factory/osmo1f5vfcph2dvfeqcqkhetwv75fda69z7e5c2dldm3kvgj23crkv6wqcn47a0/umilkTIA,string"`
	TiaPriceInUSD     float32 `json:"ibc/D79E7D83AB399BFFF93433E54FAA480C191248FC556924A2A8351AE2638B3877,string"`
}

const SQS_URL = "https://sqs.stage.osmosis.zone/tokens/usd-price-test?denoms=factory/osmo1f5vfcph2dvfeqcqkhetwv75fda69z7e5c2dldm3kvgj23crkv6wqcn47a0/umilkTIA,ibc/D79E7D83AB399BFFF93433E54FAA480C191248FC556924A2A8351AE2638B3877"

func FetchSqSData() (*SqSResponse, error) {
	resp, err := http.Get(SQS_URL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var sqsResponse SqSResponse
	if err := json.Unmarshal(body, &sqsResponse); err != nil {
		return nil, err
	}

	return &sqsResponse, nil
}
