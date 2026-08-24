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
)

func (c PipelineCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c PipelineCollector) Collect(ch chan<- prometheus.Metric) {
	bufStats := c.Buffer.Stats()
	hubStats := c.Hub.Stats()
	dedupeStats := c.Dedupe.Stats()
	divergenceStats := c.Divergence.Stats()

	// Buffer stats
	ch <- prometheus.MustNewConstMetric(
		bufferStatPushedDesc,
		prometheus.CounterValue,
		float64(bufStats.Pushed),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferStatCoalescedDesc,
		prometheus.CounterValue,
		float64(bufStats.Coalesced),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferStatEvictedDesc,
		prometheus.CounterValue,
		float64(bufStats.Evicted),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferStatTakenDesc,
		prometheus.CounterValue,
		float64(bufStats.Taken),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferStatDepthDesc,
		prometheus.GaugeValue,
		float64(bufStats.Depth),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferMaxDepthDesc,
		prometheus.GaugeValue,
		float64(bufStats.MaxDepth),
	)
	ch <- prometheus.MustNewConstMetric(
		bufferCapacityDesc,
		prometheus.GaugeValue,
		float64(bufStats.Capacity),
	)
	// Hub stats
	ch <- prometheus.MustNewConstMetric(
		hubSubscriberDesc,
		prometheus.GaugeValue,
		float64(hubStats.Subscribers),
	)
	ch <- prometheus.MustNewConstMetric(
		hubPublishedDesc,
		prometheus.CounterValue,
		float64(hubStats.Published),
	)
	ch <- prometheus.MustNewConstMetric(
		hubDroppedDesc,
		prometheus.CounterValue,
		float64(hubStats.Dropped),
	)
	ch <- prometheus.MustNewConstMetric(
		hubEvictedDesc,
		prometheus.CounterValue,
		float64(hubStats.Evicted),
	)

	// Dedupe stats
	ch <- prometheus.MustNewConstMetric(
		dedupeSeenDesc,
		prometheus.CounterValue,
		float64(dedupeStats.Seen),
	)
	ch <- prometheus.MustNewConstMetric(
		dedupeChangedDesc,
		prometheus.CounterValue,
		float64(dedupeStats.Changed),
	)
	ch <- prometheus.MustNewConstMetric(
		dedupeEvictedDesc,
		prometheus.CounterValue,
		float64(dedupeStats.Evicted),
	)
	ch <- prometheus.MustNewConstMetric(
		dedupeSizeDesc,
		prometheus.GaugeValue,
		float64(dedupeStats.Size),
	)
	ch <- prometheus.MustNewConstMetric(
		dedupeCapacityDesc,
		prometheus.GaugeValue,
		float64(dedupeStats.Capacity),
	)

	// Divergence stats
	ch <- prometheus.MustNewConstMetric(
		divergenceObservedDesc,
		prometheus.CounterValue,
		float64(divergenceStats.Observed),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceEmittedDesc,
		prometheus.CounterValue,
		float64(divergenceStats.Emitted),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceInvalidSelectionDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedInvalidSelection),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceIncompleteBookDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedIncompleteBook),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceNotCrossedDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedNotCrossed),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceSameVenueDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedSameVenue),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceBelowThresholdDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedBelowThreshold),
	)
	ch <- prometheus.MustNewConstMetric(
		divergenceStaleDesc,
		prometheus.CounterValue,
		float64(divergenceStats.SuppressedStale),
	)
}
