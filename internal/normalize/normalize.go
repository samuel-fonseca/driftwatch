package normalize

var stablecoins = map[string]string{
	"USD":   "USD",
	"USDT":  "USD",
	"USDC":  "USD",
	"FDUSD": "USD",
	"TUSD":  "USD",
	"BUSD":  "USD",
	"PYUSD": "USD",
	"RLUSD": "USD",
	"DAI":   "USD",
}

var canonicalAssetsMap = map[string]string{
	"XBT":  "BTC",
	"XXBT": "BTC",
	"ZUSD": "USD",
}

func CanonicalAsset(a string) string {
	if c, ok := canonicalAssetsMap[a]; ok {
		return c
	}
	return a
}

func Market(base, quote string) string {
	return base + "-" + quote
}

func QuoteClass(quote string) string {
	if c, ok := stablecoins[quote]; ok {
		return c
	}
	return quote
}
