package writepath

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/dashboard"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

const dashGroup = "dashboard.grafana.app"
const dashRes = "dashboards"
const testModel = "test-model"

// minimalDashboard returns a single-panel dashboard payload that the
// dashboard extractor will turn into one embed.Item.
func minimalDashboard(uid, title string) []byte {
	body, _ := json.Marshal(map[string]any{
		"uid":   uid,
		"title": title,
		"panels": []any{
			map[string]any{"id": 1, "title": "CPU", "description": "CPU usage"},
		},
	})
	return body
}

// multiPanelDashboard returns a dashboard with N panels — used to verify
// per-dashboard embedding does one EmbedText call regardless of panel count.
func multiPanelDashboard(uid, title string, n int) []byte {
	panels := make([]any, n)
	for i := 0; i < n; i++ {
		panels[i] = map[string]any{"id": i + 1, "title": uid, "description": "panel"}
	}
	body, _ := json.Marshal(map[string]any{"uid": uid, "title": title, "panels": panels})
	return body
}

// newScanner builds a Scanner without running bootstrap. Tests that
// want bootstrap should set vec.latestRV first (so bootstrap doesn't
// short-circuit on RV=0) and call s.bootstrap(ctx) explicitly.
func newScanner(t *testing.T, st *fakeStorage, vec *fakeVector) (*Scanner, *fakeText) {
	t.Helper()
	text := &fakeText{dim: 4}
	subscribe := func(ctx context.Context, _ string) (<-chan *resource.WrittenEvent, func(), error) {
		ch, err := st.WatchWriteEvents(ctx)
		if err != nil {
			return nil, nil, err
		}
		return ch, func() {}, nil
	}
	s, err := New(Options{
		Storage:       st,
		VectorBackend: vec,
		Embedder:      newFakeEmbedder(text),
		Builders:      []embed.Builder{dashboard.New()},
		Subscribe:     subscribe,
		PollInterval:  time.Hour,
	})
	require.NoError(t, err)
	return s, text
}

// dashEvent builds a pendingEvent with the dashboard group/resource pre-filled.
func dashEvent(action resourcepb.WatchEvent_Type, ns, name string, rv int64, value []byte) *pendingEvent {
	return &pendingEvent{
		action:    action,
		group:     dashGroup,
		resource:  dashRes,
		namespace: ns,
		name:      name,
		value:     value,
		rv:        rv,
	}
}

func dashChange(action resourcepb.WatchEvent_Type, ns, name string, rv int64, value []byte) *resource.ModifiedResource {
	return &resource.ModifiedResource{
		Action: action,
		Key: resourcepb.ResourceKey{
			Group: dashGroup, Resource: dashRes, Namespace: ns, Name: name,
		},
		ResourceVersion: rv,
		Value:           value,
	}
}

func TestScanner_NewValidatesInputs(t *testing.T) {
	noopSubscribe := func(context.Context, string) (<-chan *resource.WrittenEvent, func(), error) {
		return nil, func() {}, nil
	}
	cases := []struct {
		name string
		mod  func(*Options)
	}{
		{"missing storage", func(o *Options) { o.Storage = nil }},
		{"missing vector", func(o *Options) { o.VectorBackend = nil }},
		{"missing embedder", func(o *Options) { o.Embedder = nil }},
		{"missing builders", func(o *Options) { o.Builders = nil }},
		{"missing subscribe", func(o *Options) { o.Subscribe = nil }},
		{"missing embedder model", func(o *Options) {
			e := *o.Embedder
			e.Model = ""
			o.Embedder = &e
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Storage:       &fakeStorage{},
				VectorBackend: newFakeVector(),
				Embedder:      newFakeEmbedder(&fakeText{dim: 4}),
				Builders:      []embed.Builder{dashboard.New()},
				Subscribe:     noopSubscribe,
			}
			tc.mod(&opts)
			_, err := New(opts)
			require.Error(t, err)
		})
	}
}

func TestScanner_EmptyQueue_NoOp(t *testing.T) {
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, 0, text.calls)
	assert.Equal(t, int64(0), vec.latestRV)
}

func TestScanner_HappyPath_PerDashboardEmbed(t *testing.T) {
	// Two dashboards from different namespaces should produce two
	// EmbedText calls (one per dashboard) and two Upsert calls.
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")))

	s.runOnce(context.Background())

	assert.Equal(t, 2, text.calls, "one EmbedText call per dashboard")
	require.Len(t, vec.upserts, 2, "one Upsert per dashboard")
	assert.Equal(t, int64(200), vec.latestRV)
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 1, vec.lockReleases)
}

func TestScanner_MultiPanelDashboard_SingleEmbedCall(t *testing.T) {
	// All panels of one dashboard go through BatchEmbedder.Embed in
	// a single call (provider-side chunking handles panel count).
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "big", 100, multiPanelDashboard("big", "Big Dash", 12)))

	s.runOnce(context.Background())

	assert.Equal(t, 1, text.calls)
	require.Len(t, vec.upserts, 1)
	assert.Len(t, vec.upserts[0], 12, "12 panels = 12 vectors in the upsert")
}

func TestScanner_DeleteEvent_CallsVectorDelete(t *testing.T) {
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_DELETED, "ns", "dash-x", 50, nil))

	s.runOnce(context.Background())

	require.Len(t, vec.deletes, 1)
	assert.Equal(t, deleteCall{Namespace: "ns", Model: testModel, Resource: dashRes, UID: "dash-x"}, vec.deletes[0])
	assert.Equal(t, int64(50), vec.latestRV)
	assert.Equal(t, 0, text.calls, "delete does not call the embedder")
}

func TestScanner_LockUnavailable_NoWork(t *testing.T) {
	vec := newFakeVector()
	vec.lockUnavailable = true
	s, text := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Equal(t, int64(0), vec.latestRV)
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 0, vec.lockReleases)
	assert.Equal(t, 0, text.calls)
}

func TestScanner_PerEventFailure_BlocksAdvanceAtFailureRV(t *testing.T) {
	// Three dashboards: two succeed at RVs 100 and 300, one fails at 200.
	// Cursor advances to 199 so the failure is retried next cycle and
	// the unrelated successes don't get rolled back.
	vec := newFakeVector()
	vec.upsertErrFn = func(vs []vector.Vector) error {
		for _, v := range vs {
			if v.UID == "boom" {
				return errBoom
			}
		}
		return nil
	}
	s, _ := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "ok-a", 100, minimalDashboard("ok-a", "OK A")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "boom", 200, minimalDashboard("boom", "Boom")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "ok-b", 300, minimalDashboard("ok-b", "OK B")))

	s.runOnce(context.Background())

	// ok-a and ok-b succeeded; boom failed and is re-queued.
	require.Len(t, vec.upserts, 2)
	assert.Equal(t, int64(199), vec.latestRV)

	// boom should be back in the queue.
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	_, hasBoom := s.queue[eventQueueKey(dashGroup, dashRes, "ns", "boom")]
	assert.True(t, hasBoom)
}

func TestScanner_StaleSubresources_AreDeletedBeforeUpsert(t *testing.T) {
	// Pre-seed two stored panels under one dashboard, then drive an
	// update whose extract only contains panel/1.
	vec := newFakeVector()
	k := subsKey("ns", testModel, dashRes, "dash-1")
	vec.storedSubs[k] = map[string]string{
		"panel/1": "old content",
		"panel/2": "stale panel that should be deleted",
	}

	s, _ := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))
	s.runOnce(context.Background())

	require.Len(t, vec.delsubs, 1)
	assert.ElementsMatch(t, []string{"panel/2"}, vec.delsubs[0].Subresources)
	require.Len(t, vec.upserts, 1)
}

func TestScanner_MonotonicCheckpoint(t *testing.T) {
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))
	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1)
	require.Equal(t, int64(100), vec.latestRV)
	require.Equal(t, 1, text.calls)

	// Same dashboard at a higher RV: dedup keeps the new one.
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash-1", 200, minimalDashboard("dash-1", "Dash 1 v2")))
	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 2)
	require.Equal(t, int64(200), vec.latestRV)
	require.Equal(t, 2, text.calls)
}

func TestScanner_UnknownAction_BlocksAdvance(t *testing.T) {
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(&pendingEvent{
		action:    resourcepb.WatchEvent_BOOKMARK,
		group:     dashGroup,
		resource:  dashRes,
		namespace: "ns",
		name:      "weird",
		rv:        50,
	})
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, 0, text.calls)
	assert.Equal(t, int64(49), vec.latestRV, "checkpoint stops at (failed - 1)")
}

// ---------- Bootstrap ----------

func TestScanner_Bootstrap_SkipsWhenCursorIsZero(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector() // latestRV stays 0
	s, text := newScanner(t, st, vec)

	s.bootstrap(context.Background())
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "bootstrap is a no-op when cursor is 0")
	assert.Equal(t, 0, text.calls)
}

func TestScanner_Bootstrap_PullsCrossNamespaceEvents(t *testing.T) {
	// Cursor non-zero → bootstrap walks every namespace in one pass via
	// cross-namespace ListModifiedSince, enqueues each event, processes
	// them per-dashboard.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")),
	}
	vec := newFakeVector()
	vec.latestRV = 50 // anything below the change RVs
	s, text := newScanner(t, st, vec)

	s.bootstrap(context.Background())
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 2)
	assert.Equal(t, 2, text.calls)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_Bootstrap_FiltersBelowCursor(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "old", 100, minimalDashboard("old", "Old")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "new", 200, minimalDashboard("new", "New")),
	}
	vec := newFakeVector()
	vec.latestRV = 150
	s, _ := newScanner(t, st, vec)

	s.bootstrap(context.Background())
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1)
	assert.Equal(t, "new", vec.upserts[0][0].UID)
	assert.Equal(t, int64(200), vec.latestRV)
}

// ---------- Watch path ----------

func TestScanner_WatchEvent_DrivesNextCycle(t *testing.T) {
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())
	require.Empty(t, vec.upserts)
	require.Equal(t, 0, text.calls)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns-x", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1)
	assert.Equal(t, 1, text.calls)
	assert.Equal(t, int64(100), vec.latestRV)
}

func TestScanner_WatchConsumer_IgnoresUnrelatedResources(t *testing.T) {
	st := &fakeStorage{}
	vec := newFakeVector()
	s, _ := newScanner(t, st, vec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := st.WatchWriteEvents(ctx)
	require.NoError(t, err)
	go s.consumeWatchEvents(ctx, ch)

	st.emit(&resource.WrittenEvent{
		Type: resourcepb.WatchEvent_ADDED,
		Key: &resourcepb.ResourceKey{
			Group: "folder.grafana.app", Resource: "folders", Namespace: "ns-x", Name: "f1",
		},
		ResourceVersion: 50,
	})
	st.emit(&resource.WrittenEvent{
		Type: resourcepb.WatchEvent_ADDED,
		Key: &resourcepb.ResourceKey{
			Group: dashGroup, Resource: dashRes, Namespace: "ns-y", Name: "d1",
		},
		Value:           minimalDashboard("d1", "Dash 1"),
		ResourceVersion: 60,
	})

	dashKey := eventQueueKey(dashGroup, dashRes, "ns-y", "d1")
	folderKey := eventQueueKey("folder.grafana.app", "folders", "ns-x", "f1")
	require.Eventually(t, func() bool {
		s.queueMu.Lock()
		defer s.queueMu.Unlock()
		_, dashboardQueued := s.queue[dashKey]
		_, folderQueued := s.queue[folderKey]
		return dashboardQueued && !folderQueued
	}, time.Second, 10*time.Millisecond)
}

// ---------- Dedup ----------

func TestScanner_EnqueueDedup_KeepsHighestRV(t *testing.T) {
	vec := newFakeVector()
	s, text := newScanner(t, &fakeStorage{}, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Old Title")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash", 200, minimalDashboard("dash", "New Title")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Old Again")))

	s.runOnce(context.Background())

	assert.Equal(t, 1, text.calls)
	require.Len(t, vec.upserts, 1)
	require.Len(t, vec.upserts[0], 1)
	assert.Equal(t, int64(200), vec.upserts[0][0].ResourceVersion)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_EnqueueDedup_DeleteOverridesOlderUpsert(t *testing.T) {
	vec := newFakeVector()
	s, _ := newScanner(t, &fakeStorage{}, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Title")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_DELETED, "ns", "dash", 200, nil))

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "older upsert overridden by newer delete")
	require.Len(t, vec.deletes, 1)
	assert.Equal(t, "dash", vec.deletes[0].UID)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_CursorFiltersAlreadyProcessedEvents(t *testing.T) {
	vec := newFakeVector()
	vec.latestRV = 150
	s, text := newScanner(t, &fakeStorage{}, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "old", 100, minimalDashboard("old", "Old")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "new", 200, minimalDashboard("new", "New")))

	s.runOnce(context.Background())

	assert.Equal(t, 1, text.calls)
	require.Len(t, vec.upserts, 1)
	assert.Equal(t, "new", vec.upserts[0][0].UID)
	assert.Equal(t, int64(200), vec.latestRV)
}

// ---------- Retry cap ----------

func TestScanner_RetryCap_DropsEventAfterMaxAttempts(t *testing.T) {
	vec := newFakeVector()
	vec.upsertErr = errBoom
	s, _ := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "boom", 100, minimalDashboard("boom", "Boom")))

	for i := 0; i < maxEventAttempts; i++ {
		s.runOnce(context.Background())
	}

	require.Equal(t, 0, s.queueLen(), "event dropped after max attempts")
	assert.Empty(t, vec.upserts)
	// First failure pinned the cursor at lowestFailedRv-1 (= 99). On
	// the give-up cycle the failure no longer pins it, but no successful
	// event has a higher RV, so the cursor stays at 99 until something
	// past it succeeds.
	assert.Equal(t, int64(99), vec.latestRV)

	// A subsequent healthy event proves the scanner is unblocked and
	// advances the cursor.
	vec.upsertErr = nil
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns-other", "ok", 200, minimalDashboard("ok", "OK")))
	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_RetryCap_FreshHigherRVResetsBudget(t *testing.T) {
	vec := newFakeVector()
	s, _ := newScanner(t, &fakeStorage{}, vec)

	failingEv := dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "v1"))
	failingEv.attempts = maxEventAttempts - 1
	s.enqueue(failingEv)

	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash", 200, minimalDashboard("dash", "v2")))

	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1)
	assert.Equal(t, int64(200), vec.upserts[0][0].ResourceVersion)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_RetryCap_ReEnqueuePreservesAttempts(t *testing.T) {
	vec := newFakeVector()
	vec.upsertErr = errBoom
	s, _ := newScanner(t, &fakeStorage{}, vec)
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "v1")))

	s.runOnce(context.Background())
	require.Equal(t, 1, s.queueLen())

	s.queueMu.Lock()
	queued := s.queue[eventQueueKey(dashGroup, dashRes, "ns", "dash")]
	s.queueMu.Unlock()
	require.NotNil(t, queued)
	assert.Equal(t, 1, queued.attempts)
}

// ---------- chooseTarget unit ----------

func TestChooseTarget(t *testing.T) {
	const noFail = int64(1<<63 - 1)
	cases := []struct {
		name                                    string
		sinceRv, latestRv, lowestFailedRv, want int64
	}{
		{"no failures advances to latest", 50, 200, noFail, 200},
		{"failure advances to fail-1", 50, 200, 120, 119},
		{"failure at sinceRv+1 stays put", 50, 200, 51, 50},
		{"failure at sinceRv stays put", 50, 200, 50, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := chooseTarget(tc.sinceRv, tc.latestRv, tc.lowestFailedRv)
			require.Equal(t, tc.want, got)
		})
	}
}
