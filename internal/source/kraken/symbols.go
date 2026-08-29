package kraken

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const (
	defaultAssetsURL     = "https://api.kraken.com/0/public/Assets"
	defaultAssetPairsURL = "https://api.kraken.com/0/public/AssetPairs"
)

type assetPair struct {
	AltName string `json:"altname"`
	WsName  string `json:"wsname"`
	Base    string `json:"base"`
	Quote   string `json:"quote"`
	Status  string `json:"status"` // online, cancel_only
}

type asset struct {
	Class   string `json:"aclass"`
	AltName string `json:"altname"`
	Status  string `json:"status"` // enabled, withdrawal_only
}

func tickerLoader(ctx context.Context, h *poller.HTTP, assetsURL, assetPairsURL string) (symbols.Table, error) {
	panic("not implemented")
	// assetPairs, err := fetchAssetPairs(ctx, h, assetPairsURL)
	// if err != nil {
	// 	return nil, err
	// }
	// assets, err := fetchAssets(ctx, h, assetsURL)
	// if err != nil {
	// 	return nil, err
	// }

	// return nil, nil
}

func fetchAssetPairs(ctx context.Context, h *poller.HTTP, url string) ([]assetPair, error) {
	body, err := h.Get(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching asset pairs: %w", err)
	}

	var resp struct {
		Error  []string             `json:"error"`
		Result map[string]assetPair `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshalling asset pairs: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("Kraken returned: %v", resp.Error)
	}

	assetPairs := make([]assetPair, 0, len(resp.Result))
	for _, pair := range resp.Result {
		if pair.Status != "online" {
			continue
		}
		assetPairs = append(assetPairs, pair)
	}
	return assetPairs, nil
}

func fetchAssets(ctx context.Context, h *poller.HTTP, url string) ([]asset, error) {
	body, err := h.Get(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching assets: %w", err)
	}

	var resp struct {
		Error  []string         `json:"error"`
		Result map[string]asset `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshalling assets: %w", err)
	}
	if len(resp.Error) > 0 {
		return nil, fmt.Errorf("Kraken returned: %v", resp.Error)
	}

	assets := make([]asset, 0, len(resp.Result))
	for _, asset := range resp.Result {
		if asset.Status != "enabled" {
			continue
		}
		assets = append(assets, asset)
	}

	return assets, nil
}
