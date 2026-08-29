package poller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTP struct {
	Venue  string
	Client *http.Client
}

func NewHTTP(venue string, timeout time.Duration) *HTTP {
	return &HTTP{
		Venue: venue,
		Client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (h *HTTP) Get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching tickers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s returned status %d", h.Venue, resp.StatusCode)
	}

	return body, nil
}
