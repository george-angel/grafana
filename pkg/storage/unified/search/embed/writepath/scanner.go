// Package writepath keeps the vector index in sync with ongoing dashboard
// writes. The scanner combines two signals into a single in-memory queue
// keyed by (group, resource, namespace, name) and dedup'd by RV: an event
// is only kept if its RV is strictly greater than whatever the queue
// already holds for that resource. Older RVs are silently dropped at
// enqueue time, which removes the "older event overwrites newer" race
// when bootstrap and watch both surface the same dashboard.
//
// Two producers feed the queue:
//
//  1. WatchWriteEvents — a long-lived subscription (via the resource
//     server's broadcaster) that streams every dashboard write across
//     the cluster. The event payload (its Value) is enqueued directly so
//     the cycle never has to fetch it again.
//
//  2. Bootstrap (at startup, when vector_latest_rv > 0) — calls
//     ListModifiedSince with an empty namespace, walks every event past
//     the checkpoint across all namespaces in one pass, and enqueues
//     them. Catches anything that committed while the process was down
//     and isn't replayed by the new watch subscription. When the cursor
//     is 0 (fresh deployment) bootstrap is skipped entirely; the
//     backfiller, if configured, handles the initial fill and the
//     scanner just listens for new writes.
//
// Each cycle drains the queue, drops events whose RV ≤ checkpoint
// (cursor-level dedup of replayed history) and processes the rest one
// dashboard at a time: extract → cleanup-stale → BatchEmbedder.Embed →
// Upsert. Per-dashboard processing keeps the code simple; provider-side
// chunking inside EmbedText already handles panel-count batching.
//
// The scanner runs concurrently with the backfiller — there's no gating.
// The backfiller's existing Exists() check skips already-embedded
// resources, so overlap is harmless. On any per-cycle failure the
// affected events are re-enqueued (up to maxEventAttempts retries) so
// the next cycle retries them. The scanner is the sole writer of
// vector_latest_rv.
package writepath

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/embedder"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

// DefaultPollInterval is how long the scanner sleeps between cycles when
// there is no work or the lock is held by another replica.
const DefaultPollInterval = 30 * time.Second

// maxEventAttempts caps how many cycles we'll keep retrying a single
// failing event before giving up on it. With the default 30s poll
// interval that's roughly 5 minutes — long enough to ride out
// transient embedder/Vertex hiccups, short enough that a permanently
// broken dashboard doesn't block cursor advancement indefinitely.
//
// "Giving up" means the event is dropped without pinning
// lowestFailedRv, so the cursor can move past it. The dashboard's
// vector stays whatever it was; a future write to the same dashboard
// (at a higher RV) resets the attempt budget via the enqueue dedup.
const maxEventAttempts = 10

// pendingEvent is one queued change waiting to be embedded/upserted/
// deleted. Fields are flattened (rather than holding a *ResourceKey)
// because resourcepb.ResourceKey embeds protoimpl.MessageState which
// wraps a sync.Mutex — copying it triggers go vet's `copylocks`
// warning. We don't need the protobuf shape for in-memory queueing.
type pendingEvent struct {
	action    resourcepb.WatchEvent_Type
	group     string
	resource  string
	namespace string
	name      string
	value     []byte // payload for upserts; nil/empty for deletes
	rv        int64
	// attempts counts how many cycles have tried to process this event.
	// Incremented at the start of each attempt; once it reaches
	// maxEventAttempts the event is dropped without pinning the cursor.
	attempts int
}

// eventQueueKey is the dedup key — one pending event per resource at a
// time, regardless of how many writes it received.
func eventQueueKey(group, resource, namespace, name string) string {
	return group + "/" + resource + "/" + namespace + "/" + name
}

// SubscribeFunc attaches the scanner to a write-event stream. Returning
// a nil channel and a no-op release disables the watch path entirely
// (useful in tests) — bootstrap alone keeps the index correct, just
// without the per-write nudge.
//
// Called at scanner.Run() time, not at construction, so the underlying
// resource server has had a chance to initialise its broadcaster
// before the scanner actually subscribes.
type SubscribeFunc func(ctx context.Context, name string) (<-chan *resource.WrittenEvent, func(), error)

type Options struct {
	Storage       resource.StorageBackend
	VectorBackend vector.VectorBackend
	Embedder      *embedder.Embedder
	Builders      []embed.Builder
	PollInterval  time.Duration
	Log           log.Logger

	// Subscribe is the write-event source. Required.
	Subscribe SubscribeFunc
}

// Scanner is the write-path indexer. One per process; coordinated across
// replicas via vector.VectorBackend.TryAcquireScannerLock.
type Scanner struct {
	storage       resource.StorageBackend
	vectorBackend vector.VectorBackend
	embedder      *embedder.Embedder
	batchEmbedder *embedder.BatchEmbedder
	builders      map[string]embed.Builder // keyed by resource
	subscribe     SubscribeFunc
	pollInterval  time.Duration
	log           log.Logger

	// queue holds at most one pendingEvent per resource. Enqueue keeps
	// the highest RV; the cycle drains it under lock, processes, and
	// re-enqueues anything that didn't make it.
	queueMu sync.Mutex
	queue   map[string]*pendingEvent
}

func New(opts Options) (*Scanner, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("writepath: Storage is required")
	}
	if opts.VectorBackend == nil {
		return nil, fmt.Errorf("writepath: VectorBackend is required")
	}
	if opts.Embedder == nil {
		return nil, fmt.Errorf("writepath: Embedder is required")
	}
	if opts.Embedder.Model == "" {
		return nil, fmt.Errorf("writepath: Embedder.Model is required")
	}
	if len(opts.Builders) == 0 {
		return nil, fmt.Errorf("writepath: at least one Builder is required")
	}
	if opts.Subscribe == nil {
		return nil, fmt.Errorf("writepath: Subscribe is required")
	}
	builders := make(map[string]embed.Builder, len(opts.Builders))
	for _, b := range opts.Builders {
		if _, dup := builders[b.Resource()]; dup {
			return nil, fmt.Errorf("writepath: duplicate builder for resource %q", b.Resource())
		}
		builders[b.Resource()] = b
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Log == nil {
		opts.Log = log.New("writepath")
	}
	return &Scanner{
		storage:       opts.Storage,
		vectorBackend: opts.VectorBackend,
		embedder:      opts.Embedder,
		batchEmbedder: embedder.NewBatchEmbedder(*opts.Embedder),
		builders:      builders,
		subscribe:     opts.Subscribe,
		pollInterval:  opts.PollInterval,
		log:           opts.Log,
		queue:         make(map[string]*pendingEvent),
	}, nil
}

// enqueue adds an event to the queue if its RV is strictly greater than
// any pending event for the same resource. Older RVs are silently
// dropped — the dedup is the whole point of using a map keyed by
// resource identity. Events for resources without a registered builder
// or with an empty namespace (cluster-scoped) are ignored.
func (s *Scanner) enqueue(ev *pendingEvent) {
	if ev == nil || ev.namespace == "" {
		return
	}
	builder, ok := s.builders[ev.resource]
	if !ok || builder.Group() != ev.group {
		return
	}
	k := eventQueueKey(ev.group, ev.resource, ev.namespace, ev.name)
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if existing, ok := s.queue[k]; ok && existing.rv >= ev.rv {
		return
	}
	s.queue[k] = ev
}

// drainQueue returns and clears every pending event. Events that arrive
// after this call go into the next cycle; per-cycle failures re-enqueue
// specific events.
func (s *Scanner) drainQueue() []*pendingEvent {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	out := make([]*pendingEvent, 0, len(s.queue))
	for _, ev := range s.queue {
		out = append(out, ev)
	}
	s.queue = make(map[string]*pendingEvent)
	return out
}

// queueLen reports the current pending-event count (snapshot under lock).
func (s *Scanner) queueLen() int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.queue)
}

// Run subscribes to write events, bootstraps the queue, then runs the
// periodic drain-and-scan loop until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) error {
	logger := s.log.FromContext(ctx)

	resources := make([]string, 0, len(s.builders))
	for r := range s.builders {
		resources = append(resources, r)
	}
	logger.Info("writepath: scanner starting",
		"model", s.embedder.Model,
		"resources", resources,
		"poll_interval", s.pollInterval)

	// Subscribe early so events arriving during bootstrap aren't lost.
	// The broadcaster's ring-buffer cache replays recent events to the
	// new subscriber, which gives us a small natural overlap between
	// "what we caught with bootstrap" and "what watch tells us next".
	ch, release, err := s.subscribe(ctx, "vector-write-scanner")
	if err != nil {
		logger.Error("writepath: subscribe to write events", "err", err)
		// Subscribe failure isn't fatal; the periodic loop still works
		// from bootstrap output, just without per-write nudges.
	} else if ch != nil {
		defer release()
		go s.consumeWatchEvents(ctx, ch)
		logger.Info("writepath: subscribed to write events broadcaster")
	}

	s.bootstrap(ctx)

	t := time.NewTicker(s.pollInterval)
	defer t.Stop()

	// First cycle runs immediately so a freshly-started replica picks up
	// bootstrap work without waiting for the tick.
	s.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.Info("writepath: scanner stopping", "reason", ctx.Err())
			return ctx.Err()
		case <-t.C:
			s.runOnce(ctx)
		}
	}
}

// consumeWatchEvents enqueues every dashboard write the watch surfaces.
// Events for unsupported groups/resources are dropped; events with an
// older RV than what we already have queued for the same resource are
// dropped at enqueue time.
func (s *Scanner) consumeWatchEvents(ctx context.Context, ch <-chan *resource.WrittenEvent) {
	logger := s.log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				logger.Warn("writepath: watch channel closed")
				return
			}
			if ev == nil || ev.Key == nil {
				continue
			}
			s.enqueue(&pendingEvent{
				action:    ev.Type,
				group:     ev.Key.Group,
				resource:  ev.Key.Resource,
				namespace: ev.Key.Namespace,
				name:      ev.Key.Name,
				value:     ev.Value,
				rv:        ev.ResourceVersion,
			})
			logger.Debug("writepath: watch event enqueued",
				"namespace", ev.Key.Namespace,
				"name", ev.Key.Name,
				"action", ev.Type,
				"rv", ev.ResourceVersion)
		}
	}
}

// bootstrap discovers events committed past the current checkpoint and
// enqueues them via a single cross-namespace ListModifiedSince call per
// builder. Skipped entirely when the cursor is 0 — a fresh deployment
// has nothing to recover; the backfiller (if configured) handles the
// initial fill, and watch covers everything after.
func (s *Scanner) bootstrap(ctx context.Context) {
	logger := s.log.FromContext(ctx)
	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: bootstrap read checkpoint", "err", err)
		return
	}
	if sinceRv == 0 {
		logger.Info("writepath: bootstrap skipped; cursor at 0, no history to recover")
		return
	}
	logger.Info("writepath: bootstrap starting", "since_rv", sinceRv)
	before := s.queueLen()
	for _, b := range s.builders {
		if ctx.Err() != nil {
			return
		}
		s.bootstrapBuilder(ctx, b, sinceRv, logger)
	}
	logger.Info("writepath: bootstrap complete",
		"since_rv", sinceRv,
		"events_enqueued", s.queueLen()-before)
}

// bootstrapBuilder runs one cross-namespace ListModifiedSince and
// enqueues every event past sinceRv. The backend handles cross-namespace
// scans natively (empty Namespace on NamespacedResource), so there's no
// per-namespace fan-out at this layer.
func (s *Scanner) bootstrapBuilder(ctx context.Context, builder embed.Builder, sinceRv int64, logger log.Logger) {
	key := resource.NamespacedResource{
		Group:    builder.Group(),
		Resource: builder.Resource(),
		// Namespace empty → cross-namespace scan.
	}
	_, seq := s.storage.ListModifiedSince(ctx, key, sinceRv, nil)
	for mr, err := range seq {
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warn("writepath: bootstrap iterator error",
				"group", builder.Group(), "resource", builder.Resource(), "err", err)
			return
		}
		if mr == nil {
			continue
		}
		s.enqueue(&pendingEvent{
			action:    mr.Action,
			group:     mr.Key.Group,
			resource:  mr.Key.Resource,
			namespace: mr.Key.Namespace,
			name:      mr.Key.Name,
			value:     mr.Value,
			rv:        mr.ResourceVersion,
		})
	}
}

func (s *Scanner) runOnce(ctx context.Context) {
	logger := s.log.FromContext(ctx)
	release, acquired, err := s.vectorBackend.TryAcquireScannerLock(ctx)
	if err != nil {
		logger.Error("writepath: acquire lock", "err", err)
		return
	}
	if !acquired {
		logger.Debug("writepath: lock held elsewhere; skipping cycle")
		return
	}
	defer release()

	s.processQueue(ctx)
}

// processQueue drains the pending-event queue and processes each event
// one dashboard at a time: deletes call VectorBackend.Delete; updates
// extract → cleanup → BatchEmbedder.Embed → Upsert. On any per-event
// failure recordFailure decides between re-enqueueing (with the cursor
// pinned) and giving up after maxEventAttempts.
func (s *Scanner) processQueue(ctx context.Context) {
	logger := s.log.FromContext(ctx)

	pending := s.drainQueue()
	if len(pending) == 0 {
		return
	}

	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: read checkpoint", "err", err)
		s.requeue(pending)
		return
	}

	var (
		failed         []*pendingEvent
		successes      []*pendingEvent
		lowestFailedRv = int64(math.MaxInt64)
		maxRv          = sinceRv
	)

	for _, ev := range pending {
		if ctx.Err() != nil {
			s.requeue(pending)
			return
		}
		// Cursor-level dedup: anything ≤ checkpoint was already
		// processed by a prior cycle (or is replayed history from the
		// watch). Drop it without touching state.
		if ev.rv <= sinceRv {
			continue
		}
		builder, ok := s.builders[ev.resource]
		if !ok {
			continue
		}

		// Attempt budget: count this try before processing. recordFailure
		// inspects the post-increment value to decide whether to
		// re-enqueue or give up.
		ev.attempts++

		if err := s.processEvent(ctx, builder, ev); err != nil {
			logger.Warn("writepath: process event",
				"namespace", ev.namespace, "name", ev.name,
				"rv", ev.rv, "attempts", ev.attempts,
				"action", ev.action, "err", err)
			lowestFailedRv = s.recordFailure(ev, &failed, lowestFailedRv, logger)
			continue
		}
		successes = append(successes, ev)
		if ev.rv > maxRv {
			maxRv = ev.rv
		}
	}

	target := chooseTarget(sinceRv, maxRv, lowestFailedRv)
	if target > sinceRv {
		if err := s.vectorBackend.SetLatestRV(ctx, target); err != nil {
			logger.Error("writepath: advance checkpoint", "err", err, "target", target)
			// Cursor write failed; we can't tell what's persisted.
			// Re-enqueue everything we drained so nothing is dropped.
			s.requeue(pending)
			return
		}
	}

	// Re-enqueue events that didn't make the cut. The cursor advance
	// guarantees `target ≥ ev.rv` for every successful event, so the
	// cursor filter on the next cycle naturally drops anything redundant.
	for _, ev := range failed {
		s.enqueue(ev)
	}

	switch {
	case len(successes) == 0 && len(failed) == 0:
		// No-op cycle (everything dropped at the cursor). Stay quiet.
	case len(failed) == 0:
		logger.Info("writepath: cycle processed",
			"events", len(successes),
			"from", sinceRv, "to", target)
	default:
		logger.Info("writepath: cycle processed (partial)",
			"events", len(successes),
			"failed", len(failed),
			"from", sinceRv, "to", target)
	}
}

// processEvent handles one queued event end-to-end.
func (s *Scanner) processEvent(ctx context.Context, builder embed.Builder, ev *pendingEvent) error {
	switch ev.action {
	case resourcepb.WatchEvent_DELETED:
		return s.vectorBackend.Delete(ctx, ev.namespace, s.embedder.Model, builder.Resource(), ev.name)
	case resourcepb.WatchEvent_ADDED, resourcepb.WatchEvent_MODIFIED:
		return s.embedAndUpsert(ctx, builder, ev)
	default:
		return fmt.Errorf("unknown action %v", ev.action)
	}
}

// embedAndUpsert runs the per-dashboard pipeline for one event:
// extract → cleanup-stale → BatchEmbedder.Embed → Upsert.
func (s *Scanner) embedAndUpsert(ctx context.Context, builder embed.Builder, ev *pendingEvent) error {
	if len(ev.value) == 0 {
		// Empty payload on a non-delete event is treated as nothing to embed.
		return nil
	}
	key := &resourcepb.ResourceKey{
		Group:     builder.Group(),
		Resource:  builder.Resource(),
		Namespace: ev.namespace,
		Name:      ev.name,
	}
	items, err := builder.Extract(ctx, key, ev.value, "")
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	if maxItems := builder.MaxItemsPerResource(); maxItems > 0 && len(items) > maxItems {
		items = items[:maxItems]
	}

	// Drop any panel embeddings that are no longer present, before
	// upsert, so the dashboard is left in a self-consistent state if a
	// later step fails.
	if err := s.cleanupStaleSubresources(ctx, builder, ev.namespace, ev.name, items); err != nil {
		return fmt.Errorf("cleanup stale subresources: %w", err)
	}

	if len(items) == 0 {
		return nil
	}

	vectors, err := s.batchEmbedder.Embed(ctx, ev.namespace, builder.Resource(), ev.rv, items)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	if len(vectors) == 0 {
		return nil
	}
	if err := s.vectorBackend.Upsert(ctx, vectors); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// requeue puts every event back. Used when we can't tell what's
// persisted (e.g. cursor write failed) and have to assume the worst.
func (s *Scanner) requeue(events []*pendingEvent) {
	for _, ev := range events {
		s.enqueue(ev)
	}
}

// recordFailure handles a per-event processing failure. ev.attempts must
// already be incremented before calling. If the event has exhausted its
// attempt budget it's logged and dropped — the cursor is allowed to
// advance past it. Otherwise it joins the `failed` slice for
// re-enqueueing and pins the cursor below its RV.
func (s *Scanner) recordFailure(ev *pendingEvent, failed *[]*pendingEvent, lowestFailedRv int64, logger log.Logger) int64 {
	if ev.attempts >= maxEventAttempts {
		logger.Error("writepath: dropping event past retry cap; cursor will advance past it",
			"namespace", ev.namespace, "name", ev.name,
			"rv", ev.rv, "attempts", ev.attempts, "action", ev.action)
		return lowestFailedRv
	}
	*failed = append(*failed, ev)
	if ev.rv < lowestFailedRv {
		return ev.rv
	}
	return lowestFailedRv
}

// chooseTarget picks the highest checkpoint we can safely advance to:
//   - no failures: latestRv (everything in the window is durable);
//   - some failures: lowestFailedRv - 1 so the failed item is retried on
//     the next cycle (and items at lower RVs aren't reprocessed forever).
//   - the lowest failure was at sinceRv+1 (nothing safely processed):
//     return sinceRv so we don't advance.
func chooseTarget(sinceRv, latestRv, lowestFailedRv int64) int64 {
	if lowestFailedRv == math.MaxInt64 {
		return latestRv
	}
	candidate := lowestFailedRv - 1
	if candidate < sinceRv {
		return sinceRv
	}
	return candidate
}

// cleanupStaleSubresources deletes any stored subresource embeddings whose
// keys aren't represented in the latest extract. A panel that was removed
// from the dashboard would otherwise stick around in search results.
func (s *Scanner) cleanupStaleSubresources(ctx context.Context, builder embed.Builder, namespace, uid string, items []embed.Item) error {
	stored, err := s.vectorBackend.GetSubresourceContent(ctx, namespace, s.embedder.Model, builder.Resource(), uid)
	if err != nil {
		return err
	}
	if len(stored) == 0 {
		return nil
	}
	keep := make(map[string]struct{}, len(items))
	for _, it := range items {
		keep[it.Subresource] = struct{}{}
	}
	var stale []string
	for sub := range stored {
		if _, ok := keep[sub]; !ok {
			stale = append(stale, sub)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	if err := s.vectorBackend.DeleteSubresources(ctx, namespace, s.embedder.Model, builder.Resource(), uid, stale); err != nil {
		return fmt.Errorf("delete stale: %w", err)
	}
	return nil
}
