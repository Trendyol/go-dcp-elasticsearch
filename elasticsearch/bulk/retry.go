package bulk

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/valyala/fasthttp"
)

// isRetryableStatus reports whether an HTTP status code is configured as
// retryable.
func isRetryableStatus(status int, retryOn []int) bool {
	for _, s := range retryOn {
		if s == status {
			return true
		}
	}
	return false
}

// isRetryableTransportErr reports whether a transport-level error is transient
// and worth retrying (unexpected EOF, timeouts, connection failures). It is
// intentionally conservative: unknown errors are treated as non-retryable.
func isRetryableTransportErr(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}

	// fasthttp returns this sentinel (not a net.Error) when the server accepts
	// the TCP connection but closes it before sending the first response byte —
	// e.g. an Elasticsearch node that is up but has no discovered master. The
	// request never reached the server, so a bulk write is safe to retry.
	if errors.Is(err, fasthttp.ErrConnectionClosed) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

// backoffDuration returns the wait before the given retry attempt (1-based)
// using exponential growth capped at maxInterval, with full jitter to avoid
// thundering herds across pods.
func backoffDuration(attempt int, initial, maxInterval time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	backoff := initial
	for i := 1; i < attempt; i++ {
		// Cap before doubling so a large InitialInterval * 2^attempt can't
		// overflow int64 into a negative duration.
		if backoff > maxInterval/2 {
			backoff = maxInterval
			break
		}
		backoff *= 2
	}
	if backoff > maxInterval {
		backoff = maxInterval
	}

	if backoff <= 0 {
		return 0
	}

	// Full jitter: sleep a random duration in [0, backoff].
	return time.Duration(rand.Int63n(int64(backoff) + 1))
}
