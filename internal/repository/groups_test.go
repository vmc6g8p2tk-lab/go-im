package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentGroupJoinRespectsHundredMemberCap(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("set DATABASE_URL for PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &Store{DB: pool}
	suffix := time.Now().UnixNano()
	owner, err := store.CreateUser(ctx, fmt.Sprintf("cap%d_owner", suffix), "hash")
	if err != nil {
		t.Fatal(err)
	}
	group, err := store.CreateGroup(ctx, owner.ID, "capacity test")
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 105)
	for i := range ids {
		user, err := store.CreateUser(ctx, fmt.Sprintf("cap%d_%d", suffix, i), "hash")
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = user.ID
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(ids))
	for _, id := range ids {
		wg.Add(1)
		go func(id int64) { defer wg.Done(); errs <- store.JoinGroup(ctx, group.ID, id) }(id)
	}
	wg.Wait()
	close(errs)
	joined, full := 0, 0
	for err := range errs {
		if err == nil {
			joined++
		} else if errors.Is(err, ErrGroupFull) {
			full++
		} else {
			t.Fatalf("unexpected join error=%v", err)
		}
	}
	if joined != 99 || full != 6 {
		t.Fatalf("joined=%d full=%d", joined, full)
	}
	members, err := store.GroupMemberIDs(ctx, group.ID)
	if err != nil || len(members) != 100 {
		t.Fatalf("member count=%d err=%v", len(members), err)
	}
}
