package eventstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDelayedRelay_Name(t *testing.T) {
	tests := []struct {
		name      string
		relayName string
	}{
		{name: "delegates to inner relay", relayName: "my-relay"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newDelayedRelay(&delayedRelayStub{name: tc.relayName}, 10*time.Millisecond)
			if r.Name() != tc.relayName {
				t.Fatalf("expected %q, got %q", tc.relayName, r.Name())
			}
		})
	}
}

func TestDelayedRelay_Run(t *testing.T) {
	delay := 40 * time.Millisecond
	otherErr := errors.New("other error")

	tests := []struct {
		name        string
		processErr  error
		cancelAfter time.Duration
		wantErr     error
		wantDelayed bool
	}{
		{
			name:        "no delay on success",
			processErr:  nil,
			wantDelayed: false,
		},
		{
			name:        "delays on ErrEventNotReadyToProcess",
			processErr:  ErrEventNotReadyToProcess,
			wantErr:     nil,
			wantDelayed: true,
		},
		{
			name:        "propagates other errors without delay",
			processErr:  otherErr,
			wantErr:     otherErr,
			wantDelayed: false,
		},
		{
			name:        "context cancel during wait",
			processErr:  ErrEventNotReadyToProcess,
			cancelAfter: 30 * time.Millisecond,
			wantErr:     context.Canceled,
			wantDelayed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newDelayedRelay(&delayedRelayStub{name: "r", processErr: tc.processErr}, delay)

			ctx := context.Background()
			if tc.cancelAfter > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.Sleep(tc.cancelAfter)
					cancel()
				}()
			}

			start := time.Now()
			err := r.Run(ctx)
			elapsed := time.Since(start)

			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if tc.wantErr == nil && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.wantDelayed && elapsed < delay {
				t.Errorf("expected delay of at least %v, got %v", delay, elapsed)
			}
			if !tc.wantDelayed && elapsed >= delay {
				t.Errorf("expected no delay, got %v", elapsed)
			}
		})
	}
}

func TestBatchDelayedRelay_Name(t *testing.T) {
	tests := []struct {
		name      string
		relayName string
	}{
		{name: "delegates to inner relay", relayName: "my-relay"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newBatchDelayedRelay(&delayedRelayStub{name: tc.relayName}, 10*time.Millisecond)
			if r.Name() != tc.relayName {
				t.Fatalf("expected %q, got %q", tc.relayName, r.Name())
			}
		})
	}
}

func TestBatchDelayedRelay_Run(t *testing.T) {
	delay := 40 * time.Millisecond
	someErr := errors.New("some error")

	tests := []struct {
		name        string
		processErr  error
		cancelAfter time.Duration
		wantErr     error
	}{
		{
			name:       "delays after success",
			processErr: nil,
			wantErr:    nil,
		},
		{
			name:       "delays after error",
			processErr: someErr,
			wantErr:    someErr,
		},
		{
			name:        "context cancel during delay",
			processErr:  nil,
			cancelAfter: 30 * time.Millisecond,
			wantErr:     context.Canceled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newBatchDelayedRelay(&delayedRelayStub{name: "r", processErr: tc.processErr}, delay)

			ctx := context.Background()
			if tc.cancelAfter > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.Sleep(tc.cancelAfter)
					cancel()
				}()
			}

			start := time.Now()
			err := r.Run(ctx)
			elapsed := time.Since(start)

			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if tc.wantErr == nil && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if tc.cancelAfter > 0 {
				if elapsed >= delay {
					t.Errorf("expected early return due to context cancel, elapsed=%v", elapsed)
				}
			} else {
				if elapsed < delay {
					t.Errorf("expected delay of at least %v, got %v", delay, elapsed)
				}
			}
		})
	}
}
