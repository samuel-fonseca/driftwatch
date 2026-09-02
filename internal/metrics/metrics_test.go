package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samuel-fonseca/driftwatch/internal/buffer"
	"github.com/samuel-fonseca/driftwatch/internal/dedupe"
	"github.com/samuel-fonseca/driftwatch/internal/divergence"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
)

// The collector reads each stage through a Stats() interface, so the tests
// inject fakes with fixed values instead of driving real buffers. Nothing
// here may register into the package-level Registry -- a second test would
// then fail on duplicate registration.

type fakeBuffer struct{ stats buffer.Stats }

func (f fakeBuffer) Stats() buffer.Stats { return f.stats }

type fakeHub struct{ stats hub.Stats }

func (f fakeHub) Stats() hub.Stats { return f.stats }

type fakeDedupe struct{ stats dedupe.Stats }

func (f fakeDedupe) Stats() dedupe.Stats { return f.stats }

type fakeDivergence struct{ stats divergence.Stats }

func (f fakeDivergence) Stats() divergence.Stats { return f.stats }

// testCollector builds a PipelineCollector over fakes. Every field gets a
// distinct value so that emitting the wrong one -- Taken where Pushed
// belongs, say -- fails the comparison instead of silently passing.
func testCollector() PipelineCollector {
	return PipelineCollector{
		Buffer: fakeBuffer{buffer.Stats{
			Pushed:    100,
			Coalesced: 200,
			Evicted:   300,
			Taken:     400,
			Depth:     500,
			MaxDepth:  600,
			Capacity:  700,
		}},
		Hub: fakeHub{hub.Stats{
			Subscribers: 11,
			Published:   12,
			Dropped:     13,
			Evicted:     14,
		}},
		Dedupe: fakeDedupe{dedupe.Stats{
			Seen:     21,
			Changed:  22,
			Evicted:  23,
			Size:     24,
			Capacity: 25,
		}},
		Divergence: fakeDivergence{divergence.Stats{
			Observed:                   31,
			Emitted:                    32,
			SuppressedInvalidSelection: 33,
			SuppressedIncompleteBook:   34,
			SuppressedNotCrossed:       35,
			SuppressedSameVenue:        36,
			SuppressedBelowThreshold:   37,
			SuppressedStale:            38,
			SuppressedStaleArrival:     39,
			MarketsTracked:             40,
			MarketsCrossable:           41,
			SuppressedCollision:        42,
			MarketsCollided:            43,
		}},
	}
}

// wantExposition is the full expected output, sorted by metric family name.
// It pins names, HELP text, TYPE, and values in one assertion -- a counter
// that should be a gauge, or a misspelled name, fails here.
const wantExposition = `
# HELP driftwatch_buffer_capacity Capacity of the buffer
# TYPE driftwatch_buffer_capacity gauge
driftwatch_buffer_capacity 700
# HELP driftwatch_buffer_coalesced_total Number of coalesced events
# TYPE driftwatch_buffer_coalesced_total counter
driftwatch_buffer_coalesced_total 200
# HELP driftwatch_buffer_depth Number of events in the buffer
# TYPE driftwatch_buffer_depth gauge
driftwatch_buffer_depth 500
# HELP driftwatch_buffer_evicted_total Number of evicted events
# TYPE driftwatch_buffer_evicted_total counter
driftwatch_buffer_evicted_total 300
# HELP driftwatch_buffer_max_depth Maximum number of events in the buffer
# TYPE driftwatch_buffer_max_depth gauge
driftwatch_buffer_max_depth 600
# HELP driftwatch_buffer_pushed_total Number of pushed events
# TYPE driftwatch_buffer_pushed_total counter
driftwatch_buffer_pushed_total 100
# HELP driftwatch_buffer_taken_total Number of taken events
# TYPE driftwatch_buffer_taken_total counter
driftwatch_buffer_taken_total 400
# HELP driftwatch_dedupe_capacity Current dedupe capacity
# TYPE driftwatch_dedupe_capacity gauge
driftwatch_dedupe_capacity 25
# HELP driftwatch_dedupe_changed_total Total dedupe changed events
# TYPE driftwatch_dedupe_changed_total counter
driftwatch_dedupe_changed_total 22
# HELP driftwatch_dedupe_evicted_total Total dedupe evicted events
# TYPE driftwatch_dedupe_evicted_total counter
driftwatch_dedupe_evicted_total 23
# HELP driftwatch_dedupe_seen_total Total dedupe seen events
# TYPE driftwatch_dedupe_seen_total counter
driftwatch_dedupe_seen_total 21
# HELP driftwatch_dedupe_size Current dedupe size
# TYPE driftwatch_dedupe_size gauge
driftwatch_dedupe_size 24
# HELP driftwatch_divergence_emitted_total Total divergence emitted events
# TYPE driftwatch_divergence_emitted_total counter
driftwatch_divergence_emitted_total 32
# HELP driftwatch_divergence_markets_collided Markets that have tripped the collision ratio
# TYPE driftwatch_divergence_markets_collided gauge
driftwatch_divergence_markets_collided 43
# HELP driftwatch_divergence_markets_crossable Markets with a live bid and a live ask on different venues
# TYPE driftwatch_divergence_markets_crossable gauge
driftwatch_divergence_markets_crossable 41
# HELP driftwatch_divergence_markets_tracked Markets the detector currently holds quotes for
# TYPE driftwatch_divergence_markets_tracked gauge
driftwatch_divergence_markets_tracked 40
# HELP driftwatch_divergence_observed_total Total divergence observed events
# TYPE driftwatch_divergence_observed_total counter
driftwatch_divergence_observed_total 31
# HELP driftwatch_divergence_suppressed_below_threshold_total Total divergence suppressed because of below threshold events
# TYPE driftwatch_divergence_suppressed_below_threshold_total counter
driftwatch_divergence_suppressed_below_threshold_total 37
# HELP driftwatch_divergence_suppressed_collision_total Total divergence suppressed because prices disagree too far to be one asset
# TYPE driftwatch_divergence_suppressed_collision_total counter
driftwatch_divergence_suppressed_collision_total 42
# HELP driftwatch_divergence_suppressed_incomplete_book_total Total divergence suppressed because of incomplete book events
# TYPE driftwatch_divergence_suppressed_incomplete_book_total counter
driftwatch_divergence_suppressed_incomplete_book_total 34
# HELP driftwatch_divergence_suppressed_invalid_selection_total Total divergence suppressed because of invalid selection events
# TYPE driftwatch_divergence_suppressed_invalid_selection_total counter
driftwatch_divergence_suppressed_invalid_selection_total 33
# HELP driftwatch_divergence_suppressed_not_crossed_total Total divergence suppressed because of not crossed events
# TYPE driftwatch_divergence_suppressed_not_crossed_total counter
driftwatch_divergence_suppressed_not_crossed_total 35
# HELP driftwatch_divergence_suppressed_same_venue_total Total divergence suppressed because of same venue events
# TYPE driftwatch_divergence_suppressed_same_venue_total counter
driftwatch_divergence_suppressed_same_venue_total 36
# HELP driftwatch_divergence_suppressed_stale_arrival_total Total divergence suppressed because of out of order arrival events
# TYPE driftwatch_divergence_suppressed_stale_arrival_total counter
driftwatch_divergence_suppressed_stale_arrival_total 39
# HELP driftwatch_divergence_suppressed_stale_total Total divergence suppressed because of stale events
# TYPE driftwatch_divergence_suppressed_stale_total counter
driftwatch_divergence_suppressed_stale_total 38
# HELP driftwatch_hub_dropped_total Total dropped events
# TYPE driftwatch_hub_dropped_total counter
driftwatch_hub_dropped_total 13
# HELP driftwatch_hub_evicted_total Total evicted events
# TYPE driftwatch_hub_evicted_total counter
driftwatch_hub_evicted_total 14
# HELP driftwatch_hub_published_total Total published events
# TYPE driftwatch_hub_published_total counter
driftwatch_hub_published_total 12
# HELP driftwatch_hub_subscribers Total active client subscribers
# TYPE driftwatch_hub_subscribers gauge
driftwatch_hub_subscribers 11
`

// wantMetricCount is every metric the collector emits: 7 buffer + 4 hub +
// 5 dedupe + 13 divergence.
const wantMetricCount = 29

func TestCollectorExposition(t *testing.T) {
	err := testutil.CollectAndCompare(testCollector(), strings.NewReader(wantExposition))
	if err != nil {
		t.Errorf("unexpected exposition: %v", err)
	}
}

// TestCollectorEmitsEveryMetric guards against a NewDesc added without a
// matching send in Collect. That failure is otherwise silent -- the metric
// simply never shows up in a scrape.
func TestCollectorEmitsEveryMetric(t *testing.T) {
	if got := testutil.CollectAndCount(testCollector()); got != wantMetricCount {
		t.Errorf("collected %d metrics, want %d", got, wantMetricCount)
	}
}

// TestGatherAndLint enforces Prometheus naming conventions -- _total on
// counters, no type names in metric names, base units -- so metrics added
// in later stages can't drift.
func TestGatherAndLint(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(testCollector()); err != nil {
		t.Fatalf("register: %v", err)
	}

	problems, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatalf("lint: %v", err)
	}

	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
}

// TestDuplicateRegistrationFails pins the behaviour that motivates passing
// a Registerer through pipeline.Config: registering a second collector into
// the same registry is an error, so building two pipelines in one process
// panics if registration happens inside pipeline.New.
func TestDuplicateRegistrationFails(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(testCollector()); err != nil {
		t.Fatalf("first register: %v", err)
	}

	if err := reg.Register(testCollector()); err == nil {
		t.Error("second Register succeeded, want a duplicate-registration error")
	}
}

// TestRegistryHasRuntimeCollectors checks the package init wired up the Go
// and process collectors, which is where uptime and goroutine count come
// from.
func TestRegistryHasRuntimeCollectors(t *testing.T) {
	for _, name := range []string{"go_goroutines", "process_start_time_seconds"} {
		if got := testutil.CollectAndCount(Registry, name); got != 1 {
			t.Errorf("Registry exposes %d series for %s, want 1", got, name)
		}
	}
}
