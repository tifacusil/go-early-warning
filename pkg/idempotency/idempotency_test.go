package idempotency

import (
	"context"
	"testing"
)

// fakeStore is an in-memory Store used to pin the consumer's invariant: a
// message is Done only after MarkDone, so a failed (never-marked) message is
// not mistaken for a duplicate on redelivery.
type fakeStore struct{ seen map[string]bool }

func newFake() *fakeStore { return &fakeStore{seen: map[string]bool{}} }

func (f *fakeStore) Done(_ context.Context, id string) (bool, error) { return f.seen[id], nil }
func (f *fakeStore) MarkDone(_ context.Context, id string) error     { f.seen[id] = true; return nil }

func TestDoneFalseUntilMarked(t *testing.T) {
	s := newFake()
	ctx := context.Background()

	// Before processing: not done -> consumer must process, not skip.
	if done, _ := s.Done(ctx, "m1"); done {
		t.Fatal("want not-done before MarkDone")
	}
	// Simulate a failed first attempt: NO MarkDone. Redelivery must still process.
	if done, _ := s.Done(ctx, "m1"); done {
		t.Fatal("failed message must not look like a duplicate on redelivery")
	}
	// Success path: mark, then redelivery is skipped.
	if err := s.MarkDone(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if done, _ := s.Done(ctx, "m1"); !done {
		t.Fatal("want done after MarkDone")
	}
}

// fakeStore must satisfy Store.
var _ Store = (*fakeStore)(nil)
