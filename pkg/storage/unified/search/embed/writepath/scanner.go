// Package writepath keeps the vector index in sync with ongoing
// dashboard writes via a periodic scanner that drains an in-memory
// dedup queue. Watch events and bootstrap-listed events both feed the
// queue; enqueue keeps only the highest RV per resource so a replayed
// older event can't overwrite a newer one in the queue.
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

const DefaultPollInterval = 30 * time.Second

// maxEventAttempts caps retries so a permanently broken dashboard
// can't wedge cursor advancement forever. ~5 minutes at the default
// poll interval — long enough to ride out transient Vertex hiccups.
const maxEventAttempts = 10

// pendingEvent flattens (group, resource, namespace, name) instead of
// holding a *resourcepb.ResourceKey because that type embeds a sync.Mutex
// (via protoimpl.MessageState), which `go vet`'s copylocks check rejects
// on the value-typed map entries we use.
type pendingEvent struct {
	action    resourcepb.WatchEvent_Type
	group     string
	resource  string
	namespace string
	name      string
	value     []byte
	rv        int64
	attempts  int
}

func eventQueueKey(group, resource, namespace, name string) string {
	return group + "/" + resource + "/" + namespace + "/" + name
}

type Options struct {
	Storage       resource.StorageBackend
	VectorBackend vector.VectorBackend
	Embedder      *embedder.Embedder
	Builders      []embed.Builder
	PollInterval  time.Duration
	Log           log.Logger
}

// Scanner is the write-path indexer. Coordinated across replicas via
// vector.VectorBackend.TryAcquireScannerLock.
type Scanner struct {
	storage       resource.StorageBackend
	vectorBackend vector.VectorBackend
	embedder      *embedder.Embedder
	batchEmbedder *embedder.BatchEmbedder
	builders      map[string]embed.Builder
	pollInterval  time.Duration
	log           log.Logger

	// broadcaster is attached after construction by the resource server,
	// which owns its lifecycle. Signature matches
	// resource.VectorWriteScanner.UseBroadcaster so *Scanner satisfies
	// that interface.
	broadcasterMu sync.Mutex
	broadcaster   resource.Broadcaster[*resource.WrittenEvent]

	queueMu sync.Mutex
	queue   map[string]*pendingEvent
}

func (s *Scanner) UseBroadcaster(b resource.Broadcaster[*resource.WrittenEvent]) {
	s.broadcasterMu.Lock()
	defer s.broadcasterMu.Unlock()
	s.broadcaster = b
}

func (s *Scanner) currentBroadcaster() resource.Broadcaster[*resource.WrittenEvent] {
	s.broadcasterMu.Lock()
	defer s.broadcasterMu.Unlock()
	return s.broadcaster
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
		pollInterval:  opts.PollInterval,
		log:           opts.Log,
		queue:         make(map[string]*pendingEvent),
	}, nil
}

// enqueue keeps the highest RV per resource so older replayed events
// can't overwrite a newer one already queued.
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

func (s *Scanner) queueLen() int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.queue)
}

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

	// Subscribe before bootstrap so events that commit between the
	// bootstrap snapshot and the subscription join can't slip through;
	// the broadcaster's replay buffer covers the brief overlap.
	if b := s.currentBroadcaster(); b != nil {
		ch, err := b.Subscribe(ctx, "vector-write-scanner")
		if err != nil {
			logger.Error("writepath: subscribe to write events", "err", err)
		} else if ch != nil {
			defer b.Unsubscribe(ch)
			go s.consumeWatchEvents(ctx, ch)
			logger.Info("writepath: subscribed to write events broadcaster")
		}
	} else {
		logger.Warn("writepath: no broadcaster attached; running in poll-only mode")
	}

	s.bootstrap(ctx)

	t := time.NewTicker(s.pollInterval)
	defer t.Stop()

	// First cycle runs immediately so a freshly-started replica picks up
	// bootstrap work without waiting a full poll interval.
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

// bootstrap is skipped when sinceRv == 0 because a fresh deployment has
// nothing to recover — backfill (if configured) handles the initial
// fill, and watch covers everything after subscribe.
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

func (s *Scanner) bootstrapBuilder(ctx context.Context, builder embed.Builder, sinceRv int64, logger log.Logger) {
	// Empty Namespace runs cross-namespace on both backends.
	key := resource.NamespacedResource{
		Group:    builder.Group(),
		Resource: builder.Resource(),
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
		// Replayed history past the cursor was already processed; skip
		// it without spending an attempt.
		if ev.rv <= sinceRv {
			continue
		}
		builder, ok := s.builders[ev.resource]
		if !ok {
			continue
		}

		// Increment before processing so recordFailure sees the
		// post-increment value when deciding whether to retry.
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
			s.requeue(pending)
			return
		}
	}

	for _, ev := range failed {
		s.enqueue(ev)
	}

	switch {
	case len(successes) == 0 && len(failed) == 0:
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

// embedAndUpsert routes through UpsertReplaceSubresources so removing
// stale subresources and writing the new ones commit atomically — a
// failure mid-way leaves the dashboard in its previous self-consistent
// state.
//
// TODO: only re-embed subresources whose content actually changed
// since the last write. Today every dashboard write re-embeds every
// panel, which is wasteful when only one panel changed.
func (s *Scanner) embedAndUpsert(ctx context.Context, builder embed.Builder, ev *pendingEvent) error {
	if len(ev.value) == 0 {
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

	// An empty extract means the dashboard has no embeddable content;
	// drop everything stored under this UID rather than leaving orphans.
	if len(items) == 0 {
		return s.vectorBackend.Delete(ctx, ev.namespace, s.embedder.Model, builder.Resource(), ev.name)
	}

	vectors, err := s.batchEmbedder.Embed(ctx, ev.namespace, builder.Resource(), ev.rv, items)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	if len(vectors) == 0 {
		return s.vectorBackend.Delete(ctx, ev.namespace, s.embedder.Model, builder.Resource(), ev.name)
	}
	if err := s.vectorBackend.UpsertReplaceSubresources(ctx, vectors); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// requeue is the catch-all path when we can't tell what's persisted
// (e.g. cursor write failed). Successful events get filtered out by
// the cursor check on the next cycle; the wasted re-processing is
// idempotent.
func (s *Scanner) requeue(events []*pendingEvent) {
	for _, ev := range events {
		s.enqueue(ev)
	}
}

// recordFailure assumes ev.attempts has already been incremented.
// Returning lowestFailedRv unchanged on cap-exhaustion is what lets
// the cursor move past a permanently broken event.
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

// chooseTarget keeps the cursor strictly below any unhandled failure so
// the failed item is retried before the cursor moves past.
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
