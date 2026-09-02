package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/samuel-fonseca/driftwatch/internal/buffer"
	"github.com/samuel-fonseca/driftwatch/internal/dedupe"
	"github.com/samuel-fonseca/driftwatch/internal/divergence"
	"github.com/samuel-fonseca/driftwatch/internal/hub"
)

var Registry = prometheus.NewPedanticRegistry()

// init registers the default collectors and the pipeline collector.
func init() {
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

type PipelineCollector struct {
	Buffer     interface{ Stats() buffer.Stats }
	Hub        interface{ Stats() hub.Stats }
	Dedupe     interface{ Stats() dedupe.Stats }
	Divergence interface{ Stats() divergence.Stats }
}

var (
	bufferStatPushedDesc = prometheus.NewDesc(
		"driftwatch_buffer_pushed_total",
		"Number of pushed events",
		nil, nil,
	)
	bufferStatCoalescedDesc = prometheus.NewDesc(
		"driftwatch_buffer_coalesced_total",
		"Number of coalesced events",
		nil, nil,
	)
	bufferStatEvictedDesc = prometheus.NewDesc(
		"driftwatch_buffer_evicted_total",
		"Number of evicted events",
		nil, nil,
	)
	bufferStatTakenDesc = prometheus.NewDesc(
		"driftwatch_buffer_taken_total",
		"Number of taken events",
		nil, nil,
	)
	bufferStatDepthDesc = prometheus.NewDesc(
		"driftwatch_buffer_depth",
		"Number of events in the buffer",
		nil, nil,
	)
	bufferMaxDepthDesc = prometheus.NewDesc(
		"driftwatch_buffer_max_depth",
		"Maximum number of events in the buffer",
		nil, nil,
	)
	bufferCapacityDesc = prometheus.NewDesc(
		"driftwatch_buffer_capacity",
		"Capacity of the buffer",
		nil, nil,
	)

	// hub descriptions
	hubSubscriberDesc = prometheus.NewDesc(
		"driftwatch_hub_subscribers",
		"Total active client subscribers",
		nil, nil,
	)
	hubPublishedDesc = prometheus.NewDesc(
		"driftwatch_hub_published_total",
		"Total published events",
		nil, nil,
	)
	hubDroppedDesc = prometheus.NewDesc(
		"driftwatch_hub_dropped_total",
		"Total dropped events",
		nil, nil,
	)
	hubEvictedDesc = prometheus.NewDesc(
		"driftwatch_hub_evicted_total",
		"Total evicted events",
		nil, nil,
	)

	// dedupe stats
	dedupeSeenDesc = prometheus.NewDesc(
		"driftwatch_dedupe_seen_total",
		"Total dedupe seen events",
		nil, nil,
	)
	dedupeChangedDesc = prometheus.NewDesc(
		"driftwatch_dedupe_changed_total",
		"Total dedupe changed events",
		nil, nil,
	)
	dedupeEvictedDesc = prometheus.NewDesc(
		"driftwatch_dedupe_evicted_total",
		"Total dedupe evicted events",
		nil, nil,
	)
	dedupeSizeDesc = prometheus.NewDesc(
		"driftwatch_dedupe_size",
		"Current dedupe size",
		nil, nil,
	)
	dedupeCapacityDesc = prometheus.NewDesc(
		"driftwatch_dedupe_capacity",
		"Current dedupe capacity",
		nil, nil,
	)

	// divergence stats
	divergenceObservedDesc = prometheus.NewDesc(
		"driftwatch_divergence_observed_total",
		"Total divergence observed events",
		nil, nil,
	)
	divergenceEmittedDesc = prometheus.NewDesc(
		"driftwatch_divergence_emitted_total",
		"Total divergence emitted events",
		nil, nil,
	)
	divergenceInvalidSelectionDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_invalid_selection_total",
		"Total divergence suppressed because of invalid selection events",
		nil, nil,
	)
	divergenceIncompleteBookDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_incomplete_book_total",
		"Total divergence suppressed because of incomplete book events",
		nil, nil,
	)
	divergenceNotCrossedDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_not_crossed_total",
		"Total divergence suppressed because of not crossed events",
		nil, nil,
	)
	divergenceSameVenueDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_same_venue_total",
		"Total divergence suppressed because of same venue events",
		nil, nil,
	)
	divergenceBelowThresholdDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_below_threshold_total",
		"Total divergence suppressed because of below threshold events",
		nil, nil,
	)
	divergenceStaleDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_stale_total",
		"Total divergence suppressed because of stale events",
		nil, nil,
	)
	divergenceCollisionDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_collision_total",
		"Total divergence suppressed because prices disagree too far to be one asset",
		nil, nil,
	)
	divergenceMarketsCollidedDesc = prometheus.NewDesc(
		"driftwatch_divergence_markets_collided",
		"Markets that have tripped the collision ratio",
		nil, nil,
	)
	divergenceMarketsTrackedDesc = prometheus.NewDesc(
		"driftwatch_divergence_markets_tracked",
		"Markets the detector currently holds quotes for",
		nil, nil,
	)
	divergenceMarketsCrossableDesc = prometheus.NewDesc(
		"driftwatch_divergence_markets_crossable",
		"Markets with a live bid and a live ask on different venues",
		nil, nil,
	)
	divergenceStaleArrivalDesc = prometheus.NewDesc(
		"driftwatch_divergence_suppressed_stale_arrival_total",
		"Total divergence suppressed because of out of order arrival events",
		nil, nil,
	)
)

func (c PipelineCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c PipelineCollector) Collect(ch chan<- prometheus.Metric) {
	buf := c.Buffer.Stats()
	hub := c.Hub.Stats()
	dedbupe := c.Dedupe.Stats()
	divergence := c.Divergence.Stats()

	// Buffer stats
	counter(ch, bufferStatPushedDesc, uint64(buf.Pushed))
	counter(ch, bufferStatCoalescedDesc, uint64(buf.Coalesced))
	counter(ch, bufferStatEvictedDesc, uint64(buf.Evicted))
	counter(ch, bufferStatTakenDesc, uint64(buf.Taken))
	gauge(ch, bufferStatDepthDesc, buf.Depth)
	gauge(ch, bufferMaxDepthDesc, buf.MaxDepth)
	gauge(ch, bufferCapacityDesc, buf.Capacity)

	// Hub stats
	gauge(ch, hubSubscriberDesc, hub.Subscribers)
	counter(ch, hubPublishedDesc, uint64(hub.Published))
	counter(ch, hubDroppedDesc, uint64(hub.Dropped))
	counter(ch, hubEvictedDesc, uint64(hub.Evicted))

	// Dedupe stats
	counter(ch, dedupeSeenDesc, uint64(dedbupe.Seen))
	counter(ch, dedupeChangedDesc, uint64(dedbupe.Changed))
	counter(ch, dedupeEvictedDesc, uint64(dedbupe.Evicted))
	gauge(ch, dedupeSizeDesc, int(dedbupe.Size))
	gauge(ch, dedupeCapacityDesc, int(dedbupe.Capacity))

	// Divergence stats
	counter(ch, divergenceObservedDesc, uint64(divergence.Observed))
	counter(ch, divergenceEmittedDesc, uint64(divergence.Emitted))
	counter(ch, divergenceInvalidSelectionDesc, uint64(divergence.SuppressedInvalidSelection))
	counter(ch, divergenceIncompleteBookDesc, uint64(divergence.SuppressedIncompleteBook))
	counter(ch, divergenceNotCrossedDesc, uint64(divergence.SuppressedNotCrossed))
	counter(ch, divergenceSameVenueDesc, uint64(divergence.SuppressedSameVenue))
	counter(ch, divergenceBelowThresholdDesc, uint64(divergence.SuppressedBelowThreshold))
	counter(ch, divergenceStaleDesc, uint64(divergence.SuppressedStale))
	counter(ch, divergenceStaleArrivalDesc, uint64(divergence.SuppressedStaleArrival))
	gauge(ch, divergenceMarketsTrackedDesc, int(divergence.MarketsTracked))
	gauge(ch, divergenceMarketsCrossableDesc, int(divergence.MarketsCrossable))
	counter(ch, divergenceCollisionDesc, uint64(divergence.SuppressedCollision))
	gauge(ch, divergenceMarketsCollidedDesc, int(divergence.MarketsCollided))
}

func counter(ch chan<- prometheus.Metric, desc *prometheus.Desc, value uint64) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(value))
}

func gauge(ch chan<- prometheus.Metric, desc *prometheus.Desc, value int) {
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(value))
}
