package kraken

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/samuel-fonseca/driftwatch/internal/source/poller"
)

func TestParseTicks(t *testing.T) {
	serverResponse := []byte(`{"error":[],"result":{"0GEUR":{"a":["0.1390000","10456","10456.000"],"b":["0.1380000","1272","1272.000"],"c":["0.1380000","241.10222"],"v":["47716.79697","120414.57487"],"p":["0.1426522","0.1400757"],"t":[169,308],"l":["0.1380000","0.1310000"],"h":["0.1460000","0.1460000"],"o":"0.1430000"},"0GUSD":{"a":["0.1600000","11903","11903.000"],"b":["0.1590000","41019","41019.000"],"c":["0.1610000","158.73739"],"v":["338511.71862","774760.84622"],"p":["0.1657123","0.1623335"],"t":[506,889],"l":["0.1600000","0.1520000"],"h":["0.1710000","0.1710000"],"o":"0.1650000"},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["0.07550","4221","4221.000"],"c":["0.07560","27.59844000"],"v":["11667.76213764","58536.96665133"],"p":["0.07594","0.07576"],"t":[7,36],"l":["0.07550","0.07400"],"h":["0.07630","0.07780"],"o":"0.07630"}}}`)

	ticks, err := parseTicks(serverResponse)
	if err != nil {
		t.Fatalf("parseTicks: unexpected error %v", err)
	}
	if len(ticks) != 3 {
		t.Errorf("parseTicks: not enough ticks in response: got %d, want 3", len(ticks))
	}
}

func TestParseTicksHandlesErrorsInResponse(t *testing.T) {
	serverResponse := []byte(`{"error":["foo","bar"],"result":{}}`)
	ticks, err := parseTicks(serverResponse)
	if err == nil {
		t.Fatalf("parseTicks: expected error, got nil")
	}
	if err.Error() != "Kraken returned: [foo bar]" {
		t.Errorf("parseTicks: got \"%v\", want \"Kraken returned: [foo bar]\"", err)
	}
	if len(ticks) != 0 {
		t.Errorf("parseTicks: got %d, want 0", len(ticks))
	}
}

func TestParseTicksSkipsBadFloatFields(t *testing.T) {
	serverResponse := []byte(`{"error":[],"result":{"0GEUR":{"a":["non-float","10456","10456.000"],"b":["0.1380000","1272","1272.000"]},"0GUSD":{"a":["0.1600000","11903","non-float"],"b":["0.1590000","41019","41019.000"]},"1INCHEUR":{"a":["0.07560","4253","4253.000"],"b":["non-float","4221","4221.000"]},"AAVEETH":{"a":["0.05057","16","16.000"],"b":["0.05007","19","non-float"]},"AAVEUSD":{"a":["122.60000","non-float","1.000"],"b":["122.54000","non-float","11.000"]}}}`)
	ticks, err := parseTicks(serverResponse)
	if err != nil {
		t.Fatalf("parseTicks: unexpected error %v", err)
	}
	if len(ticks) != 1 {
		t.Errorf("parseTicks: too many ticks in response: got %d, want 1", len(ticks))
	}
}

func TestFetchAssetPairs(t *testing.T) {
	server := testServer(t, `{"error":[],"result":{"0GEUR":{"altname":"0GEUR","wsname":"0G/EUR","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZEUR","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.45","tick_size":"0.001","status":"online","execution_venue":"international"},"0GUSD":{"altname":"0GUSD","wsname":"0G/USD","aclass_base":"currency","base":"0G","aclass_quote":"currency","quote":"ZUSD","lot":"unit5","cost_decimals":5,"pair_decimals":3,"lot_decimals":5,"lot_multiplier":1,"leverage_buy":[2,3],"leverage_sell":[],"fees":[[0,0.4]],"fees_maker":[[0,0.23]],"fee_volume_currency":"ZUSD","margin_call":80,"margin_stop":40,"ordermin":"35","costmin":"0.5","tick_size":"0.001","status":"online","execution_venue":"international","long_position_limit":40000,"short_position_limit":0}}}`)
	h := poller.NewHTTP(venue, 10*time.Second)

	assetPairs, err := fetchAssetPairs(context.Background(), h, server.URL)
	if err != nil {
		t.Fatalf("fetchAssetPairs: %v", err)
	}
	if len(assetPairs) != 2 {
		t.Errorf("fetchAssetPairs: got %d, want 2", len(assetPairs))
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
		t.Errorf("fetchAssetPairs: got %v, want [foo bar]", err)
	}
	if len(assetPairs) != 0 {
		t.Errorf("fetchAssetPairs: got %d, want 0", len(assetPairs))
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
		t.Errorf("fetchAssetPairs: got %d, want 1", len(assetPairs))
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
		t.Errorf("fetchAssets: got %d, want 2", len(assets))
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
		t.Errorf("fetchAssets: got %v, want [foo bar]", err)
	}
	if len(assets) != 0 {
		t.Errorf("fetchAssets: got %d, want 0", len(assets))
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
		t.Errorf("fetchAssets: got %d, want 1", len(assets))
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
