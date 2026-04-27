package limits

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterAcquireReleaseFastPath(t *testing.T) {
	lim := New(1, 1)

	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire(): %v", err)
	}
	if lim.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1", lim.ActiveCount())
	}

	lim.Release()
	if lim.ActiveCount() != 0 {
		t.Errorf("ActiveCount after Release = %d, want 0", lim.ActiveCount())
	}
}

func TestLimiterQueueFull(t *testing.T) {
	lim := New(1, 1)
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire(): %v", err)
	}

	queued := make(chan error, 1)
	go func() {
		queued <- lim.Acquire(context.Background())
	}()

	deadline := time.After(2 * time.Second)
	for lim.QueuedCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for queued request")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	err := lim.Acquire(context.Background())
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Acquire() error = %v, want ErrQueueFull", err)
	}

	lim.Release()
	if err := <-queued; err != nil {
		t.Fatalf("queued Acquire() = %v, want nil", err)
	}
	lim.Release()
}

func TestLimiterAcquireCancelledWhileQueued(t *testing.T) {
	lim := New(1, 1)
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire(): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lim.Acquire(ctx)
	}()

	deadline := time.After(2 * time.Second)
	for lim.QueuedCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for queued request")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire() error = %v, want context.Canceled", err)
	}
	if lim.QueuedCount() != 0 {
		t.Errorf("QueuedCount after cancel = %d, want 0", lim.QueuedCount())
	}
	if lim.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1 for the original holder", lim.ActiveCount())
	}

	lim.Release()
}

func TestLimiterQueuedRequestAcquiresWhenSlotFrees(t *testing.T) {
	lim := New(1, 1)
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire(): %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- lim.Acquire(context.Background())
	}()

	deadline := time.After(2 * time.Second)
	for lim.QueuedCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for queued request")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	lim.Release()

	if err := <-errCh; err != nil {
		t.Fatalf("queued Acquire() = %v, want nil", err)
	}
	if lim.ActiveCount() != 1 {
		t.Errorf("ActiveCount = %d, want 1 after queued acquire succeeds", lim.ActiveCount())
	}
	if lim.QueuedCount() != 0 {
		t.Errorf("QueuedCount = %d, want 0 after queued acquire succeeds", lim.QueuedCount())
	}

	lim.Release()
}
