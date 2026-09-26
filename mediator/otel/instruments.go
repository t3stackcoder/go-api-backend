// Package otel is the OpenTelemetry glue of the framework: the instrument
// set of spec 9.1 and the adapters that feed it from the components that
// expose hooks (redisx.Consumers, pg.Idempotency, pg.Relay, and the remote
// dispatcher). The behaviors record request metrics through the same
// Instruments; the tracing behavior uses the otel trace API directly.
package otel

import (
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// MeterName is the instrumentation scope of every instrument (9.1: "all
// under the OTel meter mediator").
const MeterName = "mediator"

// Attribute keys of 9.1.
const (
	AttrName      = "name"
	AttrKind      = "kind"
	AttrOutcome   = "outcome"
	AttrEvent     = "event"
	AttrTopic     = "topic"
	AttrPartition = "partition"
	AttrGroup     = "group"
	AttrResult    = "result"
	AttrTag       = "tag"
)

// Consumer delivery outcomes of mediator.consumer.processed.
const (
	ConsumerOutcomeOK    = "ok"
	ConsumerOutcomeDedup = "dedup"
	ConsumerOutcomeError = "error"
	ConsumerOutcomeDLQ   = "dlq"
)

// Remote dispatch outcomes of mediator.remote.duration.
const (
	RemoteOutcomeOK      = "ok"
	RemoteOutcomeError   = "error"
	RemoteOutcomeTimeout = "timeout"
)

// durationBuckets are the histogram boundaries, in seconds, of the duration
// instruments; the SDK default is tuned for milliseconds.
var durationBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Instruments is every instrument of spec 9.1, created once per meter.
type Instruments struct {
	meter metric.Meter

	RequestDuration      metric.Float64Histogram   // mediator.request.duration s {name, kind, outcome}
	RequestInflight      metric.Int64UpDownCounter // mediator.request.inflight {name, kind}
	NotificationHandlers metric.Int64Histogram     // mediator.notification.handlers {event}

	OutboxUnpublished          metric.Int64ObservableGauge   // mediator.outbox.unpublished {topic, partition}
	OutboxOldestUnpublishedAge metric.Float64ObservableGauge // mediator.outbox.oldest_unpublished_age s {topic, partition}
	RelayPublished             metric.Int64ObservableCounter // mediator.relay.published {topic, partition}
	RelayReplayed              metric.Int64ObservableCounter // mediator.relay.replayed {topic, partition}

	ConsumerPending   metric.Int64Gauge   // mediator.consumer.pending {group, topic, partition}
	ConsumerLag       metric.Float64Gauge // mediator.consumer.lag s {group, topic, partition}
	ConsumerProcessed metric.Int64Counter // mediator.consumer.processed {group, outcome}
	ConsumerHalted    metric.Int64Gauge   // mediator.consumer.halted {group, topic, partition}
	LeaseOwned        metric.Int64Gauge   // mediator.lease.owned {group, topic}
	LeaseHandovers    metric.Int64Counter // mediator.lease.handovers {group, topic}

	IdempotencyOutcome      metric.Int64Counter // mediator.idempotency.outcome {name, outcome}
	CacheRequests           metric.Int64Counter // mediator.cache.requests {name, result}
	CacheInvalidationFailed metric.Int64Counter // mediator.cache.invalidation_failed {tag}
	RateLimitDecisions      metric.Int64Counter // mediator.ratelimit.decisions {name, result}
	RateLimitDegraded       metric.Int64Counter // mediator.ratelimit.degraded {name}

	RemoteDuration metric.Float64Histogram // mediator.remote.duration s {name, outcome}
	DLQSize        metric.Int64Gauge       // mediator.dlq.size {group}
}

// NewInstruments creates every instrument on meter (the global meter
// provider's "mediator" meter when nil). Every creation failure is reported
// together.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	if meter == nil {
		meter = otel.GetMeterProvider().Meter(MeterName)
	}
	i := &Instruments{meter: meter}
	var errs []error
	collect := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error

	i.RequestDuration, err = meter.Float64Histogram("mediator.request.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one request through the pipeline."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	collect(err)
	i.RequestInflight, err = meter.Int64UpDownCounter("mediator.request.inflight",
		metric.WithUnit("{request}"), metric.WithDescription("Requests currently inside the pipeline."))
	collect(err)
	i.NotificationHandlers, err = meter.Int64Histogram("mediator.notification.handlers",
		metric.WithUnit("{handler}"), metric.WithDescription("In-process handlers run per published notification."))
	collect(err)

	i.OutboxUnpublished, err = meter.Int64ObservableGauge("mediator.outbox.unpublished",
		metric.WithUnit("{row}"), metric.WithDescription("Outbox rows not yet relayed to Redis."))
	collect(err)
	i.OutboxOldestUnpublishedAge, err = meter.Float64ObservableGauge("mediator.outbox.oldest_unpublished_age",
		metric.WithUnit("s"), metric.WithDescription("Age of the oldest unpublished outbox row."))
	collect(err)
	i.RelayPublished, err = meter.Int64ObservableCounter("mediator.relay.published",
		metric.WithUnit("{row}"), metric.WithDescription("Outbox rows relayed to Redis since start."))
	collect(err)
	i.RelayReplayed, err = meter.Int64ObservableCounter("mediator.relay.replayed",
		metric.WithUnit("{row}"), metric.WithDescription("Outbox rows re-relayed after Redis data loss was detected."))
	collect(err)

	i.ConsumerPending, err = meter.Int64Gauge("mediator.consumer.pending",
		metric.WithUnit("{entry}"), metric.WithDescription("Pending stream entries of an owned partition."))
	collect(err)
	i.ConsumerLag, err = meter.Float64Gauge("mediator.consumer.lag",
		metric.WithUnit("s"), metric.WithDescription("Age of the oldest pending entry of an owned partition."))
	collect(err)
	i.ConsumerProcessed, err = meter.Int64Counter("mediator.consumer.processed",
		metric.WithUnit("{delivery}"), metric.WithDescription("Consumer deliveries by outcome."))
	collect(err)
	i.ConsumerHalted, err = meter.Int64Gauge("mediator.consumer.halted",
		metric.WithUnit("{partition}"), metric.WithDescription("1 while a partition is halted on a poison entry."))
	collect(err)
	i.LeaseOwned, err = meter.Int64Gauge("mediator.lease.owned",
		metric.WithUnit("{partition}"), metric.WithDescription("Partition leases owned by this node."))
	collect(err)
	i.LeaseHandovers, err = meter.Int64Counter("mediator.lease.handovers",
		metric.WithUnit("{handover}"), metric.WithDescription("Partition leases acquired by this node."))
	collect(err)

	i.IdempotencyOutcome, err = meter.Int64Counter("mediator.idempotency.outcome",
		metric.WithUnit("{command}"), metric.WithDescription("Keyed commands by idempotency outcome."))
	collect(err)
	i.CacheRequests, err = meter.Int64Counter("mediator.cache.requests",
		metric.WithUnit("{request}"), metric.WithDescription("Cached query lookups by result."))
	collect(err)
	i.CacheInvalidationFailed, err = meter.Int64Counter("mediator.cache.invalidation_failed",
		metric.WithUnit("{bump}"), metric.WithDescription("Tag version bumps that failed and were scheduled for retry."))
	collect(err)
	i.RateLimitDecisions, err = meter.Int64Counter("mediator.ratelimit.decisions",
		metric.WithUnit("{decision}"), metric.WithDescription("Rate limiter decisions by result."))
	collect(err)
	i.RateLimitDegraded, err = meter.Int64Counter("mediator.ratelimit.degraded",
		metric.WithUnit("{check}"), metric.WithDescription("Rate limiter checks that could not reach Redis."))
	collect(err)

	i.RemoteDuration, err = meter.Float64Histogram("mediator.remote.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of one remote dispatch."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	collect(err)
	i.DLQSize, err = meter.Int64Gauge("mediator.dlq.size",
		metric.WithUnit("{entry}"), metric.WithDescription("Length of a group's dead-letter stream."))
	collect(err)

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return i, nil
}

// Meter returns the meter the instruments were created on.
func (i *Instruments) Meter() metric.Meter { return i.meter }
