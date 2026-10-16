package event_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

func TestLifecycle_ShutdownWaitsThenRejects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		rel, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit before shutdown: %v", err)
		}

		returned := make(chan struct{})
		var shutdownErr error
		go func() {
			shutdownErr = l.Shutdown(context.Background())
			close(returned)
		}()

		synctest.Wait()
		select {
		case <-returned:
			t.Fatal("Shutdown returned while work was still in flight")
		default:
		}

		rel()
		synctest.Wait()
		select {
		case <-returned:
		default:
			t.Fatal("Shutdown did not return after the last release")
		}
		if shutdownErr != nil {
			t.Fatalf("Shutdown = %v, want nil", shutdownErr)
		}

		if _, err := l.Admit(context.Background()); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("Admit after shutdown = %v, want ErrBusClosed", err)
		}
		if _, ok := l.Track(); ok {
			t.Fatal("Track after shutdown returned ok")
		}
	})
}

func TestLifecycle_DeadlineInterruptsHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		rel, ok := l.Track()
		if !ok {
			t.Fatal("Track before shutdown refused")
		}
		hctx, stop := l.HandlerContext(context.Background(), true)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := l.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
		}
		synctest.Wait()

		if hctx.Err() == nil {
			t.Fatal("handler ctx not cancelled by the deadline")
		}
		if got := context.Cause(hctx); !errors.Is(got, event.ErrShutdownInterrupted) {
			t.Fatalf("Cause(handler ctx) = %v, want ErrShutdownInterrupted", got)
		}
		if l.Context().Err() == nil {
			t.Fatal("bus ctx not cancelled by the deadline")
		}
		wrapped := event.WrapInterrupted(hctx, hctx.Err())
		if !errors.Is(wrapped, event.ErrShutdownInterrupted) || !errors.Is(wrapped, context.Canceled) {
			t.Fatalf("WrapInterrupted = %v, want match for ErrShutdownInterrupted and context.Canceled", wrapped)
		}

		stop()
		rel()
		cancel()
	})
}
