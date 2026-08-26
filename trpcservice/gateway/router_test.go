package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestRouterHandleOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		message     InboundMessage
		cacheErr    error
		claimErr    error
		appendErr   error
		markErr     error
		want        Result
		wantError   bool
		wantMark    int
		wantRelease int
		wantAppend  int
	}{
		{
			name:    "invalid event",
			message: InboundMessage{},
			want:    Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent},
		},
		{
			name:     "unbound route",
			message:  validInbound(),
			cacheErr: tenant.ErrNotFound,
			want:     Result{Outcome: OutcomeNeedsBinding, DropReason: DropUnboundBinding},
		},
		{
			name:     "inactive binding",
			message:  validInbound(),
			cacheErr: tenant.ErrInactive,
			want:     Result{Outcome: OutcomeDropped, DropReason: DropRevokedBinding},
		},
		{
			name: "group not addressed",
			message: func() InboundMessage {
				value := validInbound()
				value.ChatType = "group"
				value.GroupID = "group-1"
				value.AddressedToBot = false
				return value
			}(),
			want: Result{Outcome: OutcomeDropped, DropReason: DropNotAddressedInGroup},
		},
		{
			name:     "Redis duplicate",
			message:  validInbound(),
			claimErr: ErrDuplicate,
			want: Result{
				Outcome: OutcomeDropped, DropReason: DropDuplicate,
				SessionID: "tenant-a:webui:user-1",
			},
		},
		{
			name:       "database duplicate",
			message:    validInbound(),
			appendErr:  storage.ErrDuplicateEvent,
			want:       Result{Outcome: OutcomeDropped, DropReason: DropDuplicate, SessionID: "tenant-a:webui:user-1"},
			wantMark:   1,
			wantAppend: 1,
		},
		{
			name:        "append failure releases claim",
			message:     validInbound(),
			appendErr:   errors.New("database unavailable"),
			wantError:   true,
			wantRelease: 1,
			wantAppend:  1,
		},
		{
			name:       "mark failure after durable append",
			message:    validInbound(),
			markErr:    errors.New("Redis unavailable"),
			wantError:  true,
			wantMark:   1,
			wantAppend: 1,
		},
		{
			name:       "ingested",
			message:    validInbound(),
			want:       Result{Outcome: OutcomeIngested, SessionID: "tenant-a:webui:user-1"},
			wantMark:   1,
			wantAppend: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			debouncer := &manualDebouncer{}
			deduper := &fakeDeduper{claimErr: test.claimErr, markErr: test.markErr}
			store := &fakeEventStore{appendErr: test.appendErr}
			router := newTestRouter(t, routerTestDeps{
				cache:     &fakeCache{snapshot: validSnapshot(), err: test.cacheErr},
				deduper:   deduper,
				debouncer: debouncer,
				store:     store,
			})

			got, err := router.Handle(context.Background(), test.message)
			if (err != nil) != test.wantError {
				t.Fatalf("Handle() error = %v, wantError = %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("Handle() = %#v, want %#v", got, test.want)
			}
			if deduper.markCalls != test.wantMark {
				t.Fatalf("Mark calls = %d, want %d", deduper.markCalls, test.wantMark)
			}
			if deduper.releaseCalls != test.wantRelease {
				t.Fatalf("Release calls = %d, want %d", deduper.releaseCalls, test.wantRelease)
			}
			if store.appendCalls != test.wantAppend {
				t.Fatalf("Append calls = %d, want %d", store.appendCalls, test.wantAppend)
			}
			if test.want.Outcome == OutcomeIngested && debouncer.scheduled != 1 {
				t.Fatalf("scheduled flushes = %d, want 1", debouncer.scheduled)
			}
		})
	}
}

func TestRouterDrainFlushesAndMarksEventsConsumed(t *testing.T) {
	debouncer := NewDebouncer(time.Hour)
	store := &fakeEventStore{
		pending: []storage.UserEvent{{
			ID: 7, SessionID: "tenant-a:webui:user-1", TenantID: "tenant-a",
			Channel: "webui", MsgID: "msg-1", SenderID: "user-1", Text: "hello",
		}},
	}
	executor := &fakeExecutor{events: []worker.Event{
		{Type: "text_delta", Text: "hello"},
		{Type: "done"},
	}}
	dispatcher := &fakeDispatcher{}
	router := newTestRouter(t, routerTestDeps{
		debouncer:  debouncer,
		store:      store,
		executor:   executor,
		dispatcher: dispatcher,
	})

	result, err := router.Handle(context.Background(), validInbound())
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if result.Outcome != OutcomeIngested {
		t.Fatalf("Handle() outcome = %q, want ingested", result.Outcome)
	}
	router.Drain()

	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if len(store.consumedIDs) != 1 || store.consumedIDs[0] != 7 {
		t.Fatalf("consumed IDs = %v, want [7]", store.consumedIDs)
	}
	if dispatcher.events != 2 {
		t.Fatalf("dispatched events = %d, want 2", dispatcher.events)
	}
	offline, err := router.Handle(context.Background(), validInbound())
	if err != nil {
		t.Fatalf("Handle() after Drain error = %v", err)
	}
	if offline.Outcome != OutcomeAgentOffline {
		t.Fatalf("Handle() after Drain outcome = %q, want agent_offline", offline.Outcome)
	}
}

func TestRouterLockHeldReschedules(t *testing.T) {
	debouncer := &manualDebouncer{}
	lock := &fakeSessionLock{err: ErrHeld}
	router := newTestRouter(t, routerTestDeps{
		debouncer: debouncer,
		lock:      lock,
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:user-1",
		}}},
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if debouncer.flush == nil {
		t.Fatal("flush was not scheduled")
	}
	firstFlush := debouncer.flush
	firstFlush()
	if debouncer.scheduled != 2 {
		t.Fatalf("scheduled flushes = %d, want retry schedule", debouncer.scheduled)
	}
}

func TestRouterInfrastructureErrors(t *testing.T) {
	t.Run("cache", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			cache: &fakeCache{err: errors.New("cache unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not return cache infrastructure error")
		}
	})

	t.Run("deduper", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			deduper: &fakeDeduper{claimErr: errors.New("Redis unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not return deduper infrastructure error")
		}
	})

	t.Run("append and release", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			deduper: &fakeDeduper{releaseErr: errors.New("release unavailable")},
			store:   &fakeEventStore{appendErr: errors.New("database unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not join append and release errors")
		}
	})
}

func TestNewRouterRejectsMissingDependencies(t *testing.T) {
	if _, err := NewRouter(
		nil, &fakeDeduper{}, &fakeSessionLock{}, &manualDebouncer{},
		&fakeEventStore{}, &fakeExecutor{}, nil, nil,
	); err == nil {
		t.Fatal("NewRouter() accepted nil cache")
	}
}

func TestDrainReplyDispatcher(t *testing.T) {
	events := make(chan worker.Event, 1)
	events <- worker.Event{Type: "done"}
	close(events)
	if err := (DrainReplyDispatcher{}).Dispatch(
		context.Background(), tenant.Snapshot{}, "session", InboundMessage{}, events,
	); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
}

func validInbound() InboundMessage {
	return InboundMessage{
		Channel:        "webui",
		RouteKey:       "binding-a",
		MsgID:          "msg-1",
		ChatType:       "p2p",
		SenderID:       "user-1",
		AddressedToBot: true,
		Text:           "hello",
		TraceID:        "trace-1",
	}
}

func validSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
			Model:    tenant.ModelConfig{Provider: "test", Model: "test-model"},
			Backends: tenant.BackendSelection{Session: "redis", Memory: "pgvector"},
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: "webui", RouteKey: "binding-a", IsActive: true,
		},
	}
}

type routerTestDeps struct {
	cache      tenant.ConfigCache
	deduper    Deduper
	lock       SessionLock
	debouncer  Debouncer
	store      storage.EventStore
	executor   worker.Executor
	dispatcher ReplyDispatcher
	audit      platformlog.AuditWriter
}

func newTestRouter(t *testing.T, deps routerTestDeps) *Router {
	t.Helper()
	if deps.cache == nil {
		deps.cache = &fakeCache{snapshot: validSnapshot()}
	}
	if deps.deduper == nil {
		deps.deduper = &fakeDeduper{}
	}
	if deps.lock == nil {
		deps.lock = &fakeSessionLock{}
	}
	if deps.debouncer == nil {
		deps.debouncer = &manualDebouncer{}
	}
	if deps.store == nil {
		deps.store = &fakeEventStore{}
	}
	if deps.executor == nil {
		deps.executor = &fakeExecutor{}
	}
	router, err := NewRouter(
		deps.cache, deps.deduper, deps.lock, deps.debouncer, deps.store,
		deps.executor, deps.dispatcher, deps.audit,
	)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}

type fakeCache struct {
	snapshot tenant.Snapshot
	err      error
}

func (f *fakeCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return f.snapshot, f.err
}
func (f *fakeCache) Invalidate(string, string) {}

type fakeDeduper struct {
	claimErr     error
	markErr      error
	releaseErr   error
	markCalls    int
	releaseCalls int
}

func (f *fakeDeduper) Claim(context.Context, string) (string, error) {
	return "owner-token", f.claimErr
}
func (f *fakeDeduper) Mark(context.Context, string, string) error {
	f.markCalls++
	return f.markErr
}
func (f *fakeDeduper) Release(context.Context, string, string) error {
	f.releaseCalls++
	return f.releaseErr
}

type fakeSessionLock struct {
	err error
}

func (f *fakeSessionLock) Acquire(context.Context, string) (Lease, error) {
	if f.err != nil {
		return nil, f.err
	}
	return fakeLease{}, nil
}

type fakeLease struct{}

func (fakeLease) Release(context.Context) error { return nil }

type manualDebouncer struct {
	scheduled int
	flush     func()
}

func (d *manualDebouncer) Schedule(_ string, flush func()) {
	d.scheduled++
	d.flush = flush
}
func (d *manualDebouncer) FlushAll() {
	if d.flush != nil {
		d.flush()
		d.flush = nil
	}
}

type fakeEventStore struct {
	appendErr   error
	pendingErr  error
	markErr     error
	appendCalls int
	pending     []storage.UserEvent
	consumedIDs []int64
}

func (f *fakeEventStore) AppendUserEvent(context.Context, storage.UserEvent) error {
	f.appendCalls++
	return f.appendErr
}
func (f *fakeEventStore) PendingUserEvents(context.Context, string, int64) ([]storage.UserEvent, error) {
	return f.pending, f.pendingErr
}
func (f *fakeEventStore) MarkConsumed(_ context.Context, ids []int64) error {
	f.consumedIDs = append([]int64(nil), ids...)
	return f.markErr
}

type fakeExecutor struct {
	mu     sync.Mutex
	events []worker.Event
	err    error
	calls  int
}

func (f *fakeExecutor) Execute(
	context.Context,
	tenant.Snapshot,
	string,
	[]storage.UserEvent,
) (<-chan worker.Event, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	result := make(chan worker.Event, len(f.events))
	for _, event := range f.events {
		result <- event
	}
	close(result)
	return result, nil
}

type fakeDispatcher struct {
	mu     sync.Mutex
	events int
}

func (f *fakeDispatcher) Dispatch(
	_ context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan worker.Event,
) error {
	for range events {
		f.mu.Lock()
		f.events++
		f.mu.Unlock()
	}
	return nil
}
