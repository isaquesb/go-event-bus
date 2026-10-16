package event_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// lrNode is a tree event: each handler run emits two children one level deeper.
type lrNode struct{ depth int }

func (e *lrNode) Name() string { return "lr.tree" }

// lrEvent counts how many times its own OnErr was called.
type lrEvent struct {
	onErr atomic.Int32
}

func (e *lrEvent) Name() string                         { return "lr.evt" }
func (e *lrEvent) OnErr(context.Context, error, string) { e.onErr.Add(1) }

// TestLocalBus_AsyncHandlerSurvivesEmitterCtx pins CORE-03 (D-09): a handler
// emitted from a request that already ended keeps the emitter's Metadata, is not
// cancelled by the emitter ctx and is drained by Shutdown.
func TestLocalBus_AsyncHandlerSurvivesEmitterCtx(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := event.NewLocalBus(event.LocalBusOptions{})

		var ran atomic.Int32
		if err := bus.Subscribe(context.Background(), "lr.evt", func(ctx context.Context, _ event.Event) error {
			time.Sleep(time.Second)
			if ctx.Err() != nil {
				t.Errorf("handler ctx cancelled by the emitter ctx: %v", ctx.Err())
			}
			md, ok := event.MetadataFromContext(ctx)
			if !ok || md.ID != "evt-a" {
				t.Errorf("Metadata = %+v ok=%v, want ID evt-a", md, ok)
			}
			ran.Add(1)
			return nil
		}, event.WithHandlerName("h")); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		emitCtx, cancel := context.WithCancel(context.Background())
		cancel() // the emitter request is already over

		bus.Emit(emitCtx, &lrEvent{}, event.WithID("evt-a"))

		if err := bus.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
		if got := ran.Load(); got != 1 {
			t.Fatalf("handler runs = %d, want 1", got)
		}
	})
}

// TestLocalBus_SelfEmitNoDeadlock proves D-10 and D-29: handlers that emit
// recursively under a single execution slot never wait for a slot, in every
// overflow mode.
func TestLocalBus_SelfEmitNoDeadlock(t *testing.T) {
	for _, mode := range []event.OverflowPolicy{
		event.OverflowBlock, event.OverflowDrop, event.OverflowUnbounded,
	} {
		synctest.Test(t, func(t *testing.T) {
			var onErr atomic.Int32
			bus := event.NewLocalBus(event.LocalBusOptions{
				MaxConcurrent: 1,
				QueueSize:     1,
				Overflow:      mode,
				OnErr: func(context.Context, event.Event, error, string) {
					onErr.Add(1)
				},
			})

			var runs atomic.Int32
			if err := bus.Subscribe(context.Background(), "lr.tree", func(ctx context.Context, e event.Event) error {
				runs.Add(1)
				n := e.(*lrNode)
				if n.depth < 4 {
					bus.Emit(ctx, &lrNode{depth: n.depth + 1})
					bus.Emit(ctx, &lrNode{depth: n.depth + 1})
				}
				return nil
			}, event.WithHandlerName("h")); err != nil {
				t.Fatalf("%s: Subscribe: %v", mode, err)
			}

			bus.Emit(context.Background(), &lrNode{})

			if err := bus.Shutdown(context.Background()); err != nil {
				t.Fatalf("%s: Shutdown = %v, want nil", mode, err)
			}
			if got := runs.Load(); got != 31 {
				t.Errorf("%s: handler runs = %d, want 31", mode, got)
			}
			if got := onErr.Load(); got != 0 {
				t.Errorf("%s: OnErr calls = %d, want 0", mode, got)
			}
		})
	}
}
