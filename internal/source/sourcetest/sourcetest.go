// Package sourcetest provides the HTTP fakes the venue packages share.
//
// Each venue's symbol loader is a GET against one URL whose body it parses,
// so their tests all need the same two servers: one that serves a fixed body
// and one that fails with a status. Keeping a single copy here means a venue
// test file contains only what is specific to that venue's payload format.
package sourcetest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Server serves body to every GET and shuts down with the test. It fails the
// test on a non-GET request: the loaders must never mutate venue state.
func Server(t *testing.T, body string) *httptest.Server {
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

// StatusServer replies to every request with code and an empty body, for the
// rate-limit and outage paths.
func StatusServer(t *testing.T, code int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
	}))
	t.Cleanup(server.Close)
	return server
}
