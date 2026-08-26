package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const executionTimeout = 10 * time.Minute

// ReplyDispatcher resolves the inbound adapter and drains the Worker event
// stream into its platform-specific reply path.
//
// SPEC-GAP: the spec's Router constructor omitted the adapter registry needed
// by its flush pseudocode. This interface makes that dependency explicit.
type ReplyDispatcher interface {
	Dispatch(
		context.Context,
		tenant.Snapshot,
		string,
		InboundMessage,
		<-chan worker.Event,
	) error
}

// DrainReplyDispatcher is used until concrete channel repliers are registered.
type DrainReplyDispatcher struct{}

// Dispatch drains all events without producing an external reply.
func (DrainReplyDispatcher) Dispatch(
	ctx context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan worker.Event,
) error {
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			for range events {
			}
			return ctx.Err()
		}
	}
}

// Router is the shared, platform-independent inbound pipeline.
type Router struct {
	cache      tenant.ConfigCache
	dedup      Deduper
	lock       SessionLock
	debouncer  Debouncer
	events     storage.EventStore
	executor   worker.Executor
	dispatcher ReplyDispatcher
	audit      platformlog.AuditWriter

	lifecycleCtx    context.Context
	cancelLifecycle context.CancelFunc
	replyWG         sync.WaitGroup

	stateMu        sync.Mutex
	stateChanged   *sync.Cond
	draining       bool
	activeHandlers int
}

// NewRouter validates and assembles the inbound pipeline.
func NewRouter(
	cache tenant.ConfigCache,
	dedup Deduper,
	lock SessionLock,
	debouncer Debouncer,
	events storage.EventStore,
	executor worker.Executor,
	dispatcher ReplyDispatcher,
	audit platformlog.AuditWriter,
) (*Router, error) {
	if cache == nil || dedup == nil || lock == nil || debouncer == nil ||
		events == nil || executor == nil {
		return nil, errors.New("router cache, deduper, lock, debouncer, event store, and executor are required")
	}
	if dispatcher == nil {
		dispatcher = DrainReplyDispatcher{}
	}
	if audit == nil {
		audit = platformlog.NopAuditWriter{}
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	router := &Router{
		cache:           cache,
		dedup:           dedup,
		lock:            lock,
		debouncer:       debouncer,
		events:          events,
		executor:        executor,
		dispatcher:      dispatcher,
		audit:           audit,
		lifecycleCtx:    lifecycleCtx,
		cancelLifecycle: cancel,
	}
	router.stateChanged = sync.NewCond(&router.stateMu)
	return router, nil
}

// Handle durably ingests a message and returns before Runner execution.
func (r *Router) Handle(ctx context.Context, message InboundMessage) (Result, error) {
	if !r.beginHandle() {
		return Result{Outcome: OutcomeAgentOffline}, nil
	}
	defer r.endHandle()
	started := time.Now()

	if err := validateInbound(message); err != nil {
		r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropInvalidEvent), "invalid_event", time.Since(started))
		return Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent}, nil
	}

	snapshot, err := r.cache.ResolveBinding(ctx, message.Channel, message.RouteKey)
	if err != nil {
		switch {
		case errors.Is(err, tenant.ErrNotFound):
			r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropUnboundBinding), "", time.Since(started))
			return Result{Outcome: OutcomeNeedsBinding, DropReason: DropUnboundBinding}, nil
		case errors.Is(err, tenant.ErrInactive):
			r.writeAudit(ctx, tenant.Snapshot{}, message, "", string(DropRevokedBinding), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropRevokedBinding}, nil
		default:
			return Result{}, fmt.Errorf("resolve channel binding: %w", err)
		}
	}
	if message.ChatType == "group" && !message.AddressedToBot {
		r.writeAudit(ctx, snapshot, message, "", string(DropNotAddressedInGroup), "", time.Since(started))
		return Result{Outcome: OutcomeDropped, DropReason: DropNotAddressedInGroup}, nil
	}

	dedupKey := BuildDedupKey(snapshot.Tenant.ID, snapshot.Binding.ID, message.MsgID)
	token, err := r.dedup.Claim(ctx, dedupKey)
	if err != nil {
		if errors.Is(err, ErrDuplicate) {
			r.writeAudit(ctx, snapshot, message, "", string(DropDuplicate), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropDuplicate}, nil
		}
		return Result{}, fmt.Errorf("claim inbound message: %w", err)
	}

	sessionID := DeriveSessionID(snapshot.Tenant.ID, message.Channel, message)
	userEvent := storage.UserEvent{
		SessionID: sessionID,
		TenantID:  snapshot.Tenant.ID,
		Channel:   message.Channel,
		MsgID:     message.MsgID,
		SenderID:  message.SenderID,
		Text:      message.Text,
		TraceID:   message.TraceID,
	}
	if err := r.events.AppendUserEvent(ctx, userEvent); err != nil {
		if errors.Is(err, storage.ErrDuplicateEvent) {
			if markErr := r.dedup.Mark(ctx, dedupKey, token); markErr != nil {
				return Result{}, fmt.Errorf("mark database-deduplicated message: %w", markErr)
			}
			r.writeAudit(ctx, snapshot, message, sessionID, string(DropDuplicate), "", time.Since(started))
			return Result{Outcome: OutcomeDropped, DropReason: DropDuplicate, SessionID: sessionID}, nil
		}
		releaseErr := r.dedup.Release(ctx, dedupKey, token)
		return Result{}, errors.Join(
			fmt.Errorf("persist inbound message: %w", err),
			wrapOptionalError("release failed inbound claim", releaseErr),
		)
	}
	if err := r.dedup.Mark(ctx, dedupKey, token); err != nil {
		return Result{}, fmt.Errorf("mark persisted inbound message: %w", err)
	}

	r.writeAudit(ctx, snapshot, message, sessionID, string(OutcomeIngested), "", time.Since(started))
	r.scheduleFlush(snapshot, sessionID, message)
	return Result{Outcome: OutcomeIngested, SessionID: sessionID}, nil
}

func (r *Router) scheduleFlush(snapshot tenant.Snapshot, sessionID string, message InboundMessage) {
	r.stateMu.Lock()
	draining := r.draining
	r.stateMu.Unlock()
	if draining {
		return
	}
	r.debouncer.Schedule(sessionID, func() {
		r.flush(snapshot, sessionID, message)
	})
}

func (r *Router) flush(snapshot tenant.Snapshot, sessionID string, message InboundMessage) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(r.lifecycleCtx, executionTimeout)
	defer cancel()

	lease, err := r.lock.Acquire(ctx, sessionID)
	if err != nil {
		if errors.Is(err, ErrHeld) {
			r.scheduleFlush(snapshot, sessionID, message)
			return
		}
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "lock_acquire", time.Since(started))
		return
	}
	defer func() {
		if err := lease.Release(context.Background()); err != nil {
			r.writeAudit(
				context.Background(), snapshot, message, sessionID,
				"execute_failed", "lock_release", time.Since(started),
			)
		}
	}()

	messages, err := r.events.PendingUserEvents(ctx, sessionID, 0)
	if err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "event_read", time.Since(started))
		return
	}
	if len(messages) == 0 {
		return
	}
	source, err := r.executor.Execute(ctx, snapshot, sessionID, messages)
	if err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "executor_start", time.Since(started))
		return
	}
	replyEvents := make(chan worker.Event, 128)
	r.replyWG.Add(1)
	go func() {
		defer r.replyWG.Done()
		if err := r.dispatcher.Dispatch(r.lifecycleCtx, snapshot, sessionID, message, replyEvents); err != nil {
			r.writeAudit(
				context.Background(), snapshot, message, sessionID,
				"reply_failed", "im_delivery", time.Since(started),
			)
		}
	}()

	forward := true
	for event := range source {
		if !forward {
			continue
		}
		select {
		case replyEvents <- event:
		case <-ctx.Done():
			forward = false
		}
	}
	close(replyEvents)

	eventIDs := make([]int64, 0, len(messages))
	for _, pending := range messages {
		eventIDs = append(eventIDs, pending.ID)
	}
	if err := r.events.MarkConsumed(ctx, eventIDs); err != nil {
		r.writeAudit(ctx, snapshot, message, sessionID, "execute_failed", "event_commit", time.Since(started))
		return
	}
	r.writeAudit(ctx, snapshot, message, sessionID, "executed", "", time.Since(started))
}

// Drain stops new ingestion, flushes pending sessions, and waits for repliers.
func (r *Router) Drain() {
	r.stateMu.Lock()
	r.draining = true
	for r.activeHandlers > 0 {
		r.stateChanged.Wait()
	}
	r.stateMu.Unlock()

	r.debouncer.FlushAll()
	r.cancelLifecycle()
	r.replyWG.Wait()
}

func (r *Router) beginHandle() bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if r.draining {
		return false
	}
	r.activeHandlers++
	return true
}

func (r *Router) endHandle() {
	r.stateMu.Lock()
	r.activeHandlers--
	if r.activeHandlers == 0 {
		r.stateChanged.Broadcast()
	}
	r.stateMu.Unlock()
}

func (r *Router) writeAudit(
	ctx context.Context,
	snapshot tenant.Snapshot,
	message InboundMessage,
	sessionID string,
	decision string,
	errorType string,
	latency time.Duration,
) {
	r.audit.Write(ctx, platformlog.AuditRecord{
		TenantID:  snapshot.Tenant.ID,
		Channel:   message.Channel,
		UserID:    message.SenderID,
		SessionID: sessionID,
		AgentName: snapshot.App.AppName,
		Decision:  decision,
		Latency:   latency,
		ErrorType: errorType,
		TraceID:   message.TraceID,
		RequestID: message.MsgID,
		TS:        time.Now(),
	})
}

func validateInbound(message InboundMessage) error {
	if message.Channel == "" || message.RouteKey == "" || message.MsgID == "" ||
		message.SenderID == "" {
		return errors.New("channel, route key, message ID, and sender ID are required")
	}
	if message.ChatType != "p2p" && message.ChatType != "group" {
		return errors.New("chat type must be p2p or group")
	}
	if message.ChatType == "group" && message.GroupID == "" {
		return errors.New("group ID is required for group messages")
	}
	return nil
}

func wrapOptionalError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
