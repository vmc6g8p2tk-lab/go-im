package presence

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRoutesExpireAndFailuresAreReported(t *testing.T) {
	server := miniredis.RunT(t)
	store := New(server.Addr(), "")
	defer store.Close()
	base := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return base }
	ctx := context.Background()
	if err := store.Touch(ctx, 7, "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Touch(ctx, 7, "two"); err != nil {
		t.Fatal(err)
	}
	instances, err := store.Instances(ctx, 7)
	if err != nil || len(instances) != 2 {
		t.Fatalf("routes=%v err=%v", instances, err)
	}
	store.now = func() time.Time { return base.Add(31 * time.Second) }
	instances, err = store.Instances(ctx, 7)
	if err != nil || len(instances) != 0 {
		t.Fatalf("stale routes=%v err=%v", instances, err)
	}
	if err := store.Touch(ctx, 7, "one"); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(ctx, 7, "one"); err != nil {
		t.Fatal(err)
	}
	instances, err = store.Instances(ctx, 7)
	if err != nil || len(instances) != 0 {
		t.Fatalf("removed routes=%v err=%v", instances, err)
	}
	server.Close()
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := store.Touch(short, 7, "one"); err == nil {
		t.Fatal("Redis outage was hidden")
	}
}
