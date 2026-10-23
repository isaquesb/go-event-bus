package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
)

func TestDelivery_Final(t *testing.T) {
	cases := []struct {
		d    event.Delivery
		want bool
	}{
		{event.Delivery{Count: 1, Max: 1}, true},
		{event.Delivery{Count: 3, Max: 3}, true},
		{event.Delivery{Count: 4, Max: 3}, true},
		{event.Delivery{Count: 1, Max: 3}, false},
		{event.Delivery{Count: 9, Max: 0}, false},
		{event.Delivery{Count: 9, Max: -1}, false},
	}
	for _, c := range cases {
		if got := c.d.Final(); got != c.want {
			t.Errorf("Delivery{Count: %d, Max: %d}.Final() = %v, want %v", c.d.Count, c.d.Max, got, c.want)
		}
	}
}

func TestDelivery_ContextRoundTrip(t *testing.T) {
	if _, ok := event.DeliveryFromContext(context.Background()); ok {
		t.Fatal("empty ctx reported a Delivery")
	}
	want := event.Delivery{
		Count:       2,
		Max:         5,
		Subject:     "orders.created",
		Raw:         []byte("raw"),
		Headers:     map[string][]string{"K": {"v"}},
		Stream:      "ORDERS",
		Consumer:    "c1",
		StreamSeq:   42,
		PublishedAt: time.Unix(1700000000, 0).UTC(),
	}
	got, ok := event.DeliveryFromContext(event.WithDelivery(context.Background(), want))
	if !ok {
		t.Fatal("Delivery not found in ctx")
	}
	if got.Count != want.Count || got.Max != want.Max || got.Subject != want.Subject ||
		string(got.Raw) != "raw" || got.Headers["K"][0] != "v" || got.Stream != want.Stream ||
		got.Consumer != want.Consumer || got.StreamSeq != want.StreamSeq || !got.PublishedAt.Equal(want.PublishedAt) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestExecution_ContextRoundTrip(t *testing.T) {
	if _, ok := event.ExecutionFromContext(context.Background()); ok {
		t.Fatal("empty ctx reported an Execution")
	}
	want := event.Execution{Handler: "h", StartedAt: time.Unix(1700000000, 0).UTC()}
	got, ok := event.ExecutionFromContext(event.WithExecution(context.Background(), want))
	if !ok {
		t.Fatal("Execution not found in ctx")
	}
	if got.Handler != want.Handler || !got.StartedAt.Equal(want.StartedAt) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}
