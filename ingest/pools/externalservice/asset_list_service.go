package externalservice

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

const ASSET_LIST_URL string = "https://raw.githubusercontent.com/osmosis-labs/assetlists/main/osmosis-1/osmosis-1.assetlist.json"

var (
	// map 'base' to array of 'denom_units'
	cache = make(map[string][]DenomUnit)
	// Mutex to ensure concurrent access to cache is safe.
	mu sync.Mutex
)

type Asset struct {
	Base       string      `json:"base"`
	DenomUnits []DenomUnit `json:"denom_units"`
}

type DenomUnit struct {
	Denom    string `json:"denom"`
	Exponent int    `json:"exponent"`
}

func FetchAssets() (map[string][]DenomUnit, error) {
	mu.Lock()
	// Return the cached data if the cache is empty
	if len(cache) > 0 {
		mu.Unlock()
		return cache, nil
	}
	mu.Unlock()

	resp, err := http.Get(ASSET_LIST_URL)
	if err != nil {
		return nil, fmt.Errorf("Error getting asset list json file: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading json file: %w", err)
	}

	var assets struct {
		Assets []Asset `json:"assets"`
	}
	if err := json.Unmarshal(body, &assets); err != nil {
		return nil, fmt.Errorf("Error parsing json file: %w", err)
	}

	mu.Lock()
	for _, asset := range assets.Assets {
		cache[asset.Base] = asset.DenomUnits
	}
	mu.Unlock()

	return cache, nil
}
