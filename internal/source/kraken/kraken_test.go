package kraken

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
)

func TestFetchAssetPairs(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{"0GEUR":{"altname":"0GEUR","wsname":"0G/EUR","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZEUR","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.45","tick_size":"0.001","status":"online","execution_venue":"international"},"0GUSD":{"altname":"0GUSD","wsname":"0G/USD","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZUSD","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[2,3],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.5","tick_size":"0.001","status":"online","execution_venue":"international","long_position_limit":40000,"short_position_limit":0}}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assetPairs, err := fetchAssetPairs(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("fetchAssetPairs: %v", err)
	}
	if len(assetPairs) != 2 {
		t.Fatalf("fetchAssetPairs: got %d, want 2", len(assetPairs))
	}
}

func TestFetchAssetPairsHandlesErrorsInResponse(t *testing.T) {
	server := testServer(t, `{"error":["foo","bar"],"result":{}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assetPairs, err := fetchAssetPairs(context.Background(), h, server.URL)
	if err == nil {
		t.Fatalf("fetchAssetPairs: expected error, got nil")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Fatalf("fetchAssetPairs: got %v, want [foo bar]", err)
	}
	if len(assetPairs) != 0 {
		t.Fatalf("fetchAssetPairs: got %d, want 0", len(assetPairs))
	}
}

func TestFetchAssetPairsSkipsNonOnlineStatus(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{"0GEUR":{"altname":"0GEUR","wsname":"0G/EUR","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZEUR","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.45","tick_size":"0.001","status":"cancel_only","execution_venue":"international"},"0GUSD":{"altname":"0GUSD","wsname":"0G/USD","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZUSD","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[2,3],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.5","tick_size":"0.001","status":"online","execution_venue":"international","long_position_limit":40000,"short_position_limit":0}}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assetPairs, err := fetchAssetPairs(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("fetchAssetPairs: %v", err)
	}
	if len(assetPairs) != 1 {
		t.Fatalf("fetchAssetPairs: got %d, want 1", len(assetPairs))
	}
}

func TestFetchAssets(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{"0G":{"aclass":"currency","altname":"0G","decimals":6,"display_decimals":4,"status":"enabled","margin_rate":"0.02"},"1INCH":{"aclass":"currency","altname":"1INCH","decimals":10,"display_decimals":5,"status":"enabled"}}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assets, err := fetchAssets(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("fetchAssets: %v", err)
	}
	if len(assets) != 2 {
		t.Fatalf("fetchAssets: got %d, want 2", len(assets))
	}
}

func TestFetchAssetsHandlesErrorsInResponse(t *testing.T) {
	server := testServer(t, `{"error":["foo","bar"],"result":{}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assets, err := fetchAssets(context.Background(), h, server.URL)
	if err == nil {
		t.Fatalf("fetchAssets: expected error, got nil")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Fatalf("fetchAssets: got %v, want [foo bar]", err)
	}
	if len(assets) != 0 {
		t.Fatalf("fetchAssets: got %d, want 0", len(assets))
	}
}

func TestFetchAssetsSkipsNonEnabledStatus(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{"0G":{"aclass":"currency","altname":"0G","decimals":6,"display_decimals":4,"status":"enabled","margin_rate":"0.02"},"1INCH":{"aclass":"currency","altname":"1INCH","decimals":10,"display_decimals":5,"status":"withdrawal_only"}}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assets, err := fetchAssets(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("fetchAssets: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("fetchAssets: got %d, want 1", len(assets))
	}
}

func testServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("request method = %q, want %q", r.Method, http.MethodGet)
		}
		w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}
