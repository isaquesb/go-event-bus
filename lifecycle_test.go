package event_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
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

// lcShutdownAsync runs Shutdown(ctx) in a goroutine and returns a func that
// reports whether it has returned, and its result.
func lcShutdownAsync(l *event.Lifecycle, ctx context.Context) func() (bool, error) {
	var (
		mu       sync.Mutex
		finished bool
		result   error
	)
	go func() {
		err := l.Shutdown(ctx)
		mu.Lock()
		finished, result = true, err
		mu.Unlock()
	}()
	return func() (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return finished, result
	}
}

func TestLifecycle_NestedAdmitDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		parent, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit parent: %v", err)
		}
		hctx, stop := l.HandlerContext(context.Background(), true)
		defer stop()

		state := lcShutdownAsync(&l, context.Background())
		synctest.Wait()

		child, err := l.Admit(hctx)
		if err != nil {
			t.Fatalf("nested Admit during drain = %v, want nil", err)
		}
		if _, err := l.Admit(context.Background()); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("external Admit during the same drain = %v, want ErrBusClosed", err)
		}

		parent()
		synctest.Wait()
		if done, _ := state(); done {
			t.Fatal("Shutdown returned while the nested child was still in flight")
		}

		child()
		synctest.Wait()
		done, err := state()
		if !done || err != nil {
			t.Fatalf("Shutdown done=%v err=%v, want done and nil", done, err)
		}
	})
}

func TestLifecycle_NestedRefusedAfterInterrupt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		parent, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit parent: %v", err)
		}
		defer parent()
		hctx, stop := l.HandlerContext(context.Background(), true)
		defer stop()

		l.BeginShutdown()
		l.Interrupt()
		synctest.Wait()

		if _, err := l.Admit(hctx); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("nested Admit after Interrupt = %v, want ErrBusClosed", err)
		}
		if _, ok := l.Track(); ok {
			t.Fatal("Track after Interrupt returned ok")
		}
	})
}

func TestLifecycle_ExternalRefusedDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l, other event.Lifecycle

		parent, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit parent: %v", err)
		}
		foreign, stopForeign := other.HandlerContext(context.Background(), true)
		defer stopForeign()

		state := lcShutdownAsync(&l, context.Background())
		synctest.Wait()

		if _, err := l.Admit(context.Background()); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("Admit with a plain ctx during drain = %v, want ErrBusClosed", err)
		}
		if _, err := l.Admit(foreign); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("Admit with another lifecycle's marker during drain = %v, want ErrBusClosed", err)
		}
		if l.Nested(foreign) {
			t.Fatal("Nested accepted the marker of another lifecycle")
		}

		parent()
		synctest.Wait()
		if done, err := state(); !done || err != nil {
			t.Fatalf("Shutdown done=%v err=%v, want done and nil", done, err)
		}
	})
}

func TestLifecycle_StaleMarkerRefusedWhenIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		// A goroutine kept the handler ctx after the handler returned: the marker
		// is stale and must not admit work once nothing is in flight.
		stale, stop := l.HandlerContext(context.Background(), true)
		defer stop()
		l.BeginShutdown()

		if _, err := l.Admit(stale); !errors.Is(err, event.ErrBusClosed) {
			t.Fatalf("Admit with a stale marker and nothing in flight = %v, want ErrBusClosed", err)
		}
	})
}

func TestLifecycle_HandlerContextDetach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		want := event.Metadata{ID: "evt-1", CorrelationID: "corr-1", Name: "thing.happened"}
		parent, cancel := context.WithTimeout(event.WithMetadata(context.Background(), want), time.Minute)
		cancel()

		detached, stopDetached := l.HandlerContext(parent, true)
		defer stopDetached()
		if err := detached.Err(); err != nil {
			t.Fatalf("detached ctx Err = %v, want nil", err)
		}
		if _, ok := detached.Deadline(); ok {
			t.Fatal("detached ctx kept the parent deadline")
		}
		md, ok := event.MetadataFromContext(detached)
		if !ok || md.ID != want.ID || md.CorrelationID != want.CorrelationID {
			t.Fatalf("detached ctx Metadata = %+v ok=%v, want %+v", md, ok, want)
		}
		if !l.Nested(detached) {
			t.Fatal("detached ctx lacks the lifecycle marker")
		}

		attached, stopAttached := l.HandlerContext(parent, false)
		defer stopAttached()
		if attached.Err() == nil {
			t.Fatal("non-detached ctx did not keep the parent cancellation")
		}
		if _, ok := attached.Deadline(); !ok {
			t.Fatal("non-detached ctx dropped the parent deadline")
		}
		if _, ok := event.MetadataFromContext(attached); !ok {
			t.Fatal("non-detached ctx dropped the Metadata")
		}
		if !l.Nested(attached) {
			t.Fatal("non-detached ctx lacks the lifecycle marker")
		}

		// Both are cancelled by Interrupt with the shutdown cause.
		live, stopLive := l.HandlerContext(context.Background(), false)
		defer stopLive()
		l.Interrupt()
		synctest.Wait()
		for name, c := range map[string]context.Context{"detached": detached, "live": live} {
			if c.Err() == nil || !errors.Is(context.Cause(c), event.ErrShutdownInterrupted) {
				t.Fatalf("%s ctx: Err=%v Cause=%v, want cancelled with ErrShutdownInterrupted", name, c.Err(), context.Cause(c))
			}
		}
	})
}

func TestLifecycle_ZeroValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		if l.Closing() {
			t.Fatal("Closing true before any shutdown")
		}
		if l.Context() == nil {
			t.Fatal("Context nil on a zero Lifecycle")
		}
		if err := l.Context().Err(); err != nil {
			t.Fatalf("bus ctx Err = %v before shutdown, want nil", err)
		}
		if err := l.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown with nothing in flight = %v, want nil", err)
		}
		if !l.Closing() {
			t.Fatal("Closing false after Shutdown")
		}
	})
}

func TestLifecycle_ShutdownIdempotentConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		rel, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		first := lcShutdownAsync(&l, context.Background())
		second := lcShutdownAsync(&l, context.Background())
		synctest.Wait()
		if done, _ := first(); done {
			t.Fatal("first Shutdown returned before the release")
		}
		if done, _ := second(); done {
			t.Fatal("second Shutdown returned before the release")
		}

		rel()
		synctest.Wait()
		for name, state := range map[string]func() (bool, error){"first": first, "second": second} {
			if done, err := state(); !done || err != nil {
				t.Fatalf("%s Shutdown done=%v err=%v, want done and nil", name, done, err)
			}
		}
		if err := l.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown after a completed one = %v, want nil", err)
		}
	})
}

func TestLifecycle_ReleaseIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		a, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit a: %v", err)
		}
		b, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit b: %v", err)
		}

		state := lcShutdownAsync(&l, context.Background())
		synctest.Wait()

		a()
		a()
		synctest.Wait()
		if done, _ := state(); done {
			t.Fatal("a double release decremented twice: Shutdown returned with b in flight")
		}

		b()
		synctest.Wait()
		if done, err := state(); !done || err != nil {
			t.Fatalf("Shutdown done=%v err=%v, want done and nil", done, err)
		}
	})
}

func TestLifecycle_GoexitReleases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		rel, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		exited := make(chan struct{})
		go func() {
			defer close(exited)
			defer rel()
			runtime.Goexit()
		}()
		<-exited

		if err := l.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown after a Goexit holder = %v, want nil", err)
		}
	})
}

func TestLifecycle_TrackDuringGracefulDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l event.Lifecycle

		first, err := l.Admit(context.Background())
		if err != nil {
			t.Fatalf("Admit: %v", err)
		}
		l.BeginShutdown()

		waited := make(chan error, 1)
		go func() { waited <- l.Wait(context.Background()) }()
		synctest.Wait()

		second, ok := l.Track()
		if !ok {
			t.Fatal("Track refused during a graceful drain")
		}

		first()
		synctest.Wait()
		select {
		case err := <-waited:
			t.Fatalf("Wait returned %v while a tracked message was in flight", err)
		default:
		}

		second()
		synctest.Wait()
		select {
		case err := <-waited:
			if err != nil {
				t.Fatalf("Wait = %v, want nil", err)
			}
		default:
			t.Fatal("Wait did not return after the last release")
		}

		if _, ok := l.Track(); ok {
			t.Fatal("Track succeeded after Wait observed idle")
		}
	})
}

func TestWrapInterrupted(t *testing.T) {
	var l event.Lifecycle
	hctx, stop := l.HandlerContext(context.Background(), true)
	defer stop()
	l.Interrupt()
	<-hctx.Done()

	callerCtx, callerCancel := context.WithCancel(context.Background())
	callerCancel()

	plain := errors.New("boom")
	already := event.WrapInterrupted(hctx, context.Canceled)

	t.Run("nil stays nil", func(t *testing.T) {
		if got := event.WrapInterrupted(hctx, nil); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})
	t.Run("plain error unchanged", func(t *testing.T) {
		if got := event.WrapInterrupted(hctx, plain); got != plain {
			t.Fatalf("got %v, want the same error", got)
		}
	})
	t.Run("caller cancellation unchanged", func(t *testing.T) {
		if got := event.WrapInterrupted(callerCtx, callerCtx.Err()); got != callerCtx.Err() {
			t.Fatalf("got %v, want the same context.Canceled", got)
		}
	})
	t.Run("canceled by shutdown wrapped", func(t *testing.T) {
		got := event.WrapInterrupted(hctx, hctx.Err())
		if !errors.Is(got, event.ErrShutdownInterrupted) || !errors.Is(got, context.Canceled) {
			t.Fatalf("got %v, want match for ErrShutdownInterrupted and context.Canceled", got)
		}
	})
	t.Run("deadline exceeded by shutdown wrapped", func(t *testing.T) {
		got := event.WrapInterrupted(hctx, context.DeadlineExceeded)
		if !errors.Is(got, event.ErrShutdownInterrupted) || !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("got %v, want match for ErrShutdownInterrupted and context.DeadlineExceeded", got)
		}
	})
	t.Run("already wrapped unchanged", func(t *testing.T) {
		if got := event.WrapInterrupted(hctx, already); got != already {
			t.Fatalf("got %v, want the same single wrap %v", got, already)
		}
	})
	t.Run("non context error on interrupted ctx unchanged", func(t *testing.T) {
		if got := event.WrapInterrupted(hctx, plain); errors.Is(got, event.ErrShutdownInterrupted) {
			t.Fatalf("got %v, a plain error must not be reclassified", got)
		}
	})
}

// TestLifecycle_RaceStress runs in real time, outside any synctest bubble.
func TestLifecycle_RaceStress(t *testing.T) {
	var l event.Lifecycle
	const workers = 100

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				rel, err := l.Admit(context.Background())
				if err != nil {
					if !errors.Is(err, event.ErrBusClosed) {
						t.Errorf("Admit = %v, want nil or ErrBusClosed", err)
					}
					return
				}
				hctx, stop := l.HandlerContext(context.Background(), i%2 == 0)
				if child, err := l.Admit(hctx); err == nil {
					child()
				}
				if track, ok := l.Track(); ok {
					track()
				}
				stop()
				rel()
			}
		}(i)
	}

	close(start)
	time.Sleep(time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v, want nil", err)
	}
	wg.Wait()

	if _, err := l.Admit(context.Background()); !errors.Is(err, event.ErrBusClosed) {
		t.Fatalf("final Admit = %v, want ErrBusClosed", err)
	}
}
