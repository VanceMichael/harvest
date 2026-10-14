/*
Copyright NetApp Inc, 2021 All rights reserved
*/

package delivery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDoClassifiesStatusCodes(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		header     string
		wantRetry  bool
		wantReject bool
		wantAfter  time.Duration
	}{
		{"204 accepted", http.StatusNoContent, "", false, false, 0},
		{"200 accepted", http.StatusOK, "", false, false, 0},
		{"500 retryable", http.StatusInternalServerError, "", true, false, 0},
		{"502 retryable", http.StatusBadGateway, "", true, false, 0},
		{"503 honors retry-after", http.StatusServiceUnavailable, "3", true, false, 3 * time.Second},
		{"429 retryable", http.StatusTooManyRequests, "", true, false, 0},
		{"408 retryable", http.StatusRequestTimeout, "", true, false, 0},
		{"400 rejected", http.StatusBadRequest, "", false, true, 0},
		{"401 rejected", http.StatusUnauthorized, "", false, true, 0},
		{"403 rejected", http.StatusForbidden, "", false, true, 0},
		{"413 rejected", http.StatusRequestEntityTooLarge, "", false, true, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("body"))
			}))
			defer srv.Close()

			req, err := http.NewRequest(http.MethodPost, srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = Do(context.Background(), srv.Client(), req)
			if tc.wantRetry {
				if !errors.Is(err, ErrRetryable) {
					t.Fatalf("want ErrRetryable, got %v", err)
				}
				var se *Error
				if errors.As(err, &se) && se.RetryAfter() != tc.wantAfter {
					t.Fatalf("retry-after = %v, want %v", se.RetryAfter(), tc.wantAfter)
				}
			} else if tc.wantReject {
				if !errors.Is(err, ErrRejected) {
					t.Fatalf("want ErrRejected, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
}

func TestDoTransportFailureIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	url := srv.URL
	srv.Close() // connection refused

	req, _ := http.NewRequest(http.MethodPost, url, nil)
	err := Do(context.Background(), &http.Client{Timeout: 2 * time.Second}, req)
	if !errors.Is(err, ErrRetryable) {
		t.Fatalf("transport error must be retryable, got %v", err)
	}
	if errors.Is(err, ErrRejected) {
		t.Fatalf("transport error must not be rejected")
	}
}

func TestBackoffDeterministic(t *testing.T) {
	min, max := 10*time.Millisecond, 100*time.Millisecond
	got := []time.Duration{
		backoff(min, max, 1),
		backoff(min, max, 2),
		backoff(min, max, 3),
		backoff(min, max, 4),
		backoff(min, max, 10),
	}
	want := []time.Duration{10, 20, 40, 80, 100}
	for i := range got {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("attempt %d backoff = %v, want %v", i+1, got[i], want[i]*time.Millisecond)
		}
	}
}
