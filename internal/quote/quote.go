package quote

import (
	"fmt"
	"hash/fnv"
	"math"
	"time"
)

type Quote struct {
	Venue      string    `json:"venue"`
	Market     string    `json:"market"`
	Selection  string    `json:"selection"`
	Price      float64   `json:"price"`
	Size       float64   `json:"size"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (q Quote) Key() string {
	return fmt.Sprintf("%s|%s|%s", q.Venue, q.Market, q.Selection)
}

func (q Quote) MarketKey() string {
	return fmt.Sprintf("%s|%s", q.Market, q.Selection)
}

func (q Quote) Fingerprint() uint64 {
	h := fnv.New64()
	h.Write([]byte(fmt.Appendf(
		nil,
		"%x|%x",
		math.Float64bits(q.Price),
		math.Float64bits(q.Size),
	)))
	return h.Sum64()
}
