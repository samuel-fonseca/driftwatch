// Package hubtest reads the hub's SSE stream from a test.
//
// Both the hub's own tests and the pipeline's end-to-end tests need to assert
// on what a subscriber actually receives. Hand-rolled bufio scanning in each
// of them drifted into scanning for a substring anywhere in the stream, and
// into deadline loops that spin hot when the read fails. A real frame parser
// makes the assertions exact -- an event's name and payload, a heartbeat --
// and makes waiting a blocking receive rather than a poll.
package hubtest

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A Frame is one SSE event: the lines up to a blank separator line.
type Frame struct {
	Event    string   // the "event:" field, empty for an unnamed event
	Data     string   // the "data:" field
	Retry    string   // the "retry:" field
	Comments []string // ":"-prefixed lines, e.g. "connected", "heartbeat"
}

// IsComment reports whether the frame carried only comments -- the connect
// preamble and heartbeats, neither of which is a published event.
func (f Frame) IsComment() bool {
	return f.Event == "" && f.Data == "" && len(f.Comments) > 0
}

// A Stream is a subscribed SSE client.
type Stream struct {
	t      *testing.T
	frames chan Frame
	errs   chan error
	cancel context.CancelFunc
	Resp   *http.Response
}

// Subscribe issues a GET against url and starts parsing frames in the
// background. The connection and the parser are torn down with the test.
func Subscribe(t *testing.T, url string) *Stream {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("building request for %s: %v", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("subscribing to %s: %v", url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	s := &Stream{
		t:      t,
		frames: make(chan Frame, 64),
		errs:   make(chan error, 1),
		cancel: cancel,
		Resp:   resp,
	}

	go func() {
		reader := bufio.NewReader(resp.Body)
		for {
			frame, err := readFrame(reader)
			if err != nil {
				select {
				case s.errs <- err:
				default:
				}
				return
			}
			select {
			case s.frames <- frame:
			case <-ctx.Done():
				return
			}
		}
	}()

	return s
}

// readFrame accumulates lines until the blank line that ends an SSE frame.
func readFrame(reader *bufio.Reader) (Frame, error) {
	var frame Frame
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return Frame{}, err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return frame, nil
		}

		switch {
		case strings.HasPrefix(line, ":"):
			frame.Comments = append(frame.Comments, strings.TrimSpace(line[1:]))
		case strings.HasPrefix(line, "event:"):
			frame.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			frame.Data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		case strings.HasPrefix(line, "retry:"):
			frame.Retry = strings.TrimSpace(strings.TrimPrefix(line, "retry:"))
		}
	}
}

// Next returns the next frame, failing the test if none arrives in time.
func (s *Stream) Next(timeout time.Duration) Frame {
	s.t.Helper()

	select {
	case frame := <-s.frames:
		return frame
	case err := <-s.errs:
		s.t.Fatalf("reading SSE stream: %v", err)
	case <-time.After(timeout):
		s.t.Fatalf("no SSE frame arrived within %v", timeout)
	}
	return Frame{}
}

// NextEvent returns the next frame carrying an actual event, skipping the
// connect preamble and any heartbeats.
func (s *Stream) NextEvent(timeout time.Duration) Frame {
	s.t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			s.t.Fatal("no SSE event arrived before the deadline")
		}
		if frame := s.Next(remaining); !frame.IsComment() {
			return frame
		}
	}
}

// ExpectNoEvent fails the test if an event (not a comment) arrives within d.
func (s *Stream) ExpectNoEvent(d time.Duration) {
	s.t.Helper()

	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		select {
		case frame := <-s.frames:
			if !frame.IsComment() {
				s.t.Fatalf("unexpected SSE event: %+v", frame)
			}
		case <-s.errs:
			return
		case <-time.After(remaining):
			return
		}
	}
}

// Close ends the subscription.
func (s *Stream) Close() { s.cancel() }
