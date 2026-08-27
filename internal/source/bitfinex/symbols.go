package bitfinex

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samuel-fonseca/driftwatch/internal/normalize"
	"github.com/samuel-fonseca/driftwatch/internal/symbols"
)

const (
	testPrefix = "TEST"
	confURL    = "https://api-pub.bitfinex.com/v2/conf/" +
		"pub:list:pair:exchange,pub:list:currency,pub:map:currency:sym"
)

type conf struct {
	pairs,
	currencies []string
	aliases map[string]string
}

// tickerLoader satisfies symbols.Loader against Bitfinex's public conf API.
func (a *Adapter) tickerLoader(ctx context.Context) (symbols.Table, error) {
	body, err := a.get(ctx, a.confURL)
	if err != nil {
		return nil, fmt.Errorf("loading conf: %w", err)
	}

	c, err := parseConf(body)
	if err != nil {
		return nil, fmt.Errorf("parsing conf: %w", err)
	}

	return buildTable(c), nil
}

func parseConf(body []byte) (*conf, error) {
	var sections []json.RawMessage
	if err := json.Unmarshal(body, &sections); err != nil {
		return nil, fmt.Errorf("unmarshalling conf: %w", err)
	}
	if len(sections) != 3 {
		return nil, fmt.Errorf("conf returned %d sections, want 3", len(sections))
	}

	var pairs, currencies []string
	var aliasRows [][]string

	if err := json.Unmarshal(sections[0], &pairs); err != nil {
		return nil, fmt.Errorf("unmarshalling pairs: %w", err)
	}
	if err := json.Unmarshal(sections[1], &currencies); err != nil {
		return nil, fmt.Errorf("unmarshalling currencies: %w", err)
	}
	if err := json.Unmarshal(sections[2], &aliasRows); err != nil {
		return nil, fmt.Errorf("unmarshalling aliasRows: %w", err)
	}

	if len(pairs) == 0 || len(currencies) == 0 || len(aliasRows) == 0 {
		return nil, fmt.Errorf(
			"conf sections empty (pairs=%d currencies=%d aliases=%d) -- check the conf keys",
			len(pairs), len(currencies), len(aliasRows),
		)
	}

	aliases := make(map[string]string, len(aliasRows))
	for _, row := range aliasRows {
		if len(row) != 2 {
			continue
		}
		aliases[row[0]] = row[1]
	}

	return &conf{pairs: pairs, currencies: currencies, aliases: aliases}, nil
}

func buildTable(c *conf) symbols.Table {
	known := make(map[string]struct{}, len(c.currencies))
	for _, cur := range c.currencies {
		known[cur] = struct{}{}
	}

	table := make(symbols.Table, len(c.pairs))
	for _, pair := range c.pairs {
		base, quoteAsset, ok := splitPair(pair, known)
		if !ok {
			continue
		}
		if isTestAsset(base) || isTestAsset(quoteAsset) {
			continue
		}

		base = canonicalAsset(base, c.aliases)
		quoteAsset = canonicalAsset(quoteAsset, c.aliases)

		symbol := "t" + pair
		table[symbol] = symbols.Instrument{
			Symbol: symbol,
			Base:   base,
			Quote:  quoteAsset,
			Market: normalize.Market(base, quoteAsset),
		}
	}
	return table
}

func isTestAsset(s string) bool { return strings.HasPrefix(s, testPrefix) }

func canonicalAsset(a string, aliases map[string]string) string {
	if alias, ok := aliases[a]; ok {
		a = strings.ToUpper(alias)
	}
	return normalize.CanonicalAsset(a)
}

func splitPair(pair string, known map[string]struct{}) (base, quoteAsset string, ok bool) {
	if b, q, found := strings.Cut(pair, ":"); found {
		return b, q, b != "" && q != ""
	}

	for i := range len(pair) {
		b, q := pair[:i], pair[i:]
		if _, found := known[b]; !found {
			continue
		}
		if _, found := known[q]; found {
			return b, q, true
		}
	}
	return "", "", false
}
