package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatchDelayedRelay_Name(t *testing.T) {
	stub := &delayedRelayStub{name: "my-relay"}
	r := newBatchDelayedRelay(stub, 10*time.Millisecond)
	if r.Name() != "my-relay" {
		t.Fatalf("expected name %q, got %q", "my-relay", r.Name())
	}
}

func TestBatchDelayedRelay_DelaysAfterSuccess(t *testing.T) {
	delay := 40 * time.Millisecond
	r := newBatchDelayedRelay(&delayedRelayStub{name: "r", processErr: nil}, delay)
	start := time.Now()
	err := r.Run(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if elapsed < delay {
		t.Fatalf("expected delay of at least %v, got %v", delay, elapsed)
	}
}

func TestBatchDelayedRelay_DelaysAfterError(t *testing.T) {
	delay := 40 * time.Millisecond
	expected := errors.New("some error")
	r := newBatchDelayedRelay(&delayedRelayStub{name: "r", processErr: expected}, delay)
	start := time.Now()
	err := r.Run(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, expected) {
		t.Fatalf("expected error to propagate, got %v", err)
	}
	if elapsed < delay {
		t.Fatalf("expected delay even on error, got %v", elapsed)
	}
}

func TestBatchDelayedRelay_ContextCancelDuringDelay(t *testing.T) {
	delay := 200 * time.Millisecond
	r := newBatchDelayedRelay(&delayedRelayStub{name: "r", processErr: nil}, delay)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := r.Run(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed >= delay {
		t.Fatalf("expected early return due to context cancel, elapsed=%v", elapsed)
	}
}

func TestDelayedRelay_Name(t *testing.T) {
	stub := &delayedRelayStub{name: "my-relay"}
	r := newDelayedRelay(stub, 10*time.Millisecond)
	if r.Name() != "my-relay" {
		t.Fatalf("expected name %q, got %q", "my-relay", r.Name())
	}
}

func TestDelayedRelay_NoDelayOnSuccess(t *testing.T) {
	delay := 100 * time.Millisecond
	r := newDelayedRelay(&delayedRelayStub{name: "r", processErr: nil}, delay)
	start := time.Now()
	err := r.Run(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if elapsed >= delay {
		t.Fatalf("expected no delay on success, got %v", elapsed)
	}
}