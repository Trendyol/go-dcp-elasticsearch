package bulk

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func Test_isRetryableStatus(t *testing.T) {
	retryOn := []int{429, 502, 503, 504}

	cases := map[int]bool{
		429: true,
		503: true,
		504: true,
		400: false,
		404: false,
		200: false,
	}
	for status, want := range cases {
		if got := isRetryableStatus(status, retryOn); got != want {
			t.Fatalf("isRetryableStatus(%d) = %v, want %v", status, got, want)
		}
	}

	if isRetryableStatus(429, nil) {
		t.Fatal("empty retryOn must never be retryable")
	}
}

func Test_isRetryableTransportErr(t *testing.T) {
	if isRetryableTransportErr(nil) {
		t.Fatal("nil error must not be retryable")
	}
	if !isRetryableTransportErr(io.ErrUnexpectedEOF) {
		t.Fatal("unexpected EOF must be retryable")
	}
	if isRetryableTransportErr(io.ErrNoProgress) {
		t.Fatal("unknown error must not be retryable")
	}
}

func Test_backoffDuration(t *testing.T) {
	initial := 100 * time.Millisecond
	maxInterval := 1 * time.Second

	// Full jitter means the result is within [0, cap] where cap grows
	// exponentially but never exceeds maxInterval.
	for attempt := 1; attempt <= 6; attempt++ {
		d := backoffDuration(attempt, initial, maxInterval)
		if d < 0 {
			t.Fatalf("attempt %d: negative backoff %v", attempt, d)
		}
		if d > maxInterval {
			t.Fatalf("attempt %d: backoff %v exceeds max %v", attempt, d, maxInterval)
		}
	}

	// Zero initial interval yields no wait.
	if d := backoffDuration(3, 0, maxInterval); d != 0 {
		t.Fatalf("zero initial interval must yield 0 backoff, got %v", d)
	}
}

func Test_parseBulkItemErrors(t *testing.T) {
	t.Run("no_errors_returns_nil", func(t *testing.T) {
		body := `{"took":1,"errors":false,"items":[{"index":{"_index":"idx","_id":"1","status":201}}]}`
		out, err := parseBulkItemErrors(newResponse(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != nil {
			t.Fatalf("expected nil outcomes, got %v", out)
		}
	})

	t.Run("extracts_failed_items_with_status", func(t *testing.T) {
		body := `{"took":1,"errors":true,"items":[` +
			`{"index":{"_index":"idx","_id":"1","status":429,"error":{"type":"es_rejected","reason":"busy"}}},` +
			`{"index":{"_index":"idx","_id":"2","status":201}},` +
			`{"index":{"_index":"idx","_id":"3","status":400,"error":{"type":"mapper","reason":"bad"}}}` +
			`]}`
		out, err := parseBulkItemErrors(newResponse(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("expected 2 failed items, got %d", len(out))
		}
		if out[0].position != 0 || out[0].status != 429 {
			t.Fatalf("first failure = pos %d status %d, want pos 0 status 429", out[0].position, out[0].status)
		}
		if out[1].position != 2 || out[1].status != 400 {
			t.Fatalf("second failure = pos %d status %d, want pos 2 status 400", out[1].position, out[1].status)
		}
	})
}

func newResponse(body string) *esapi.Response {
	return &esapi.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
