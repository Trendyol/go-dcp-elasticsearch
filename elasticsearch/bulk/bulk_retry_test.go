package bulk

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/client"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	"github.com/Trendyol/go-dcp-elasticsearch/helper"
	esv7 "github.com/elastic/go-elasticsearch/v7"
)

// recordingHandler captures per-item outcomes. finalizeProcess invokes the
// handler synchronously, so slices are fully populated once requestFuncWithRetry
// returns.
type recordingHandler struct {
	mu      sync.Mutex
	success []string
	errored []string
}

func (h *recordingHandler) OnSuccess(ctx *elasticsearch.SinkResponseHandlerContext) {
	h.mu.Lock()
	h.success = append(h.success, string(ctx.Action.ID))
	h.mu.Unlock()
}

func (h *recordingHandler) OnError(ctx *elasticsearch.SinkResponseHandlerContext) {
	h.mu.Lock()
	h.errored = append(h.errored, string(ctx.Action.ID))
	h.mu.Unlock()
}

func (h *recordingHandler) OnInit(*elasticsearch.SinkResponseHandlerInitContext)       {}
func (h *recordingHandler) OnBeforeBulk(*elasticsearch.SinkResponseHandlerBulkContext) {}
func (h *recordingHandler) OnAfterBulk(*elasticsearch.SinkResponseHandlerBulkContext)  {}

func fastRetry() *config.Retry {
	return &config.Retry{
		Enabled:         true,
		MaxRetries:      2,
		RetryOnStatus:   []int{429, 503},
		InitialInterval: time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
	}
}

func indexItem(id string) *elasticsearch.BatchItem {
	return &elasticsearch.BatchItem{
		Action: &document.ESActionDocument{ID: []byte(id), IndexName: "idx", Type: document.Index},
		Bytes:  []byte(`{"index":{"_index":"idx","_id":"` + id + `"}}` + "\n" + `{"v":1}` + "\n"),
	}
}

func buildBulk(esClient *esv7.Client, handler *recordingHandler) *Bulk {
	return &Bulk{
		readers:           []*helper.MultiDimByteReader{helper.NewMultiDimByteReader(nil)},
		concurrentRequest: 1,
		esClients:         map[string]*esv7.Client{"": esClient},
		metric: &Metric{
			IndexingSuccessActionCounter: map[string]int64{},
			IndexingErrorActionCounter:   map[string]int64{},
			DeletionSuccessActionCounter: map[string]int64{},
			DeletionErrorActionCounter:   map[string]int64{},
		},
		sinkResponseHandler: handler,
	}
}

// esClientForServer builds a v7 client pointed at the test server with node
// discovery disabled so no background requests are issued.
func esClientForServer(t *testing.T, url string) *esv7.Client {
	t.Helper()
	interval := time.Hour
	c, err := client.NewElasticClientFromElasticsearch(&config.Elasticsearch{
		Urls:                        []string{url},
		DisableDiscoverNodesOnStart: true,
		DiscoverNodesInterval:       &interval,
		MaxRetries:                  1,
	})
	if err != nil {
		t.Fatalf("build es client: %v", err)
	}
	return c
}

func elasticProduct(w http.ResponseWriter) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
}

func Test_requestFuncWithRetry_RetryableItemThenSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elasticProduct(w)
		if !strings.Contains(r.URL.Path, "_bulk") {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if atomic.AddInt32(&calls, 1) == 1 {
			// item "1" transiently rejected, item "2" ok
			_, _ = w.Write([]byte(`{"errors":true,"items":[` +
				`{"index":{"_index":"idx","_id":"1","status":429,"error":{"reason":"busy"}}},` +
				`{"index":{"_index":"idx","_id":"2","status":201}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"errors":false}`))
	}))
	defer srv.Close()

	handler := &recordingHandler{}
	b := buildBulk(esClientForServer(t, srv.URL), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1"), indexItem("2")}, b.esClients[""], fastRetry())()
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected 2 bulk calls, got %d", got)
	}
	if len(handler.errored) != 0 {
		t.Fatalf("expected no errored items, got %v", handler.errored)
	}
	if len(handler.success) != 2 {
		t.Fatalf("expected 2 successful items, got %v", handler.success)
	}
}

func Test_requestFuncWithRetry_TerminalNotRetried(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elasticProduct(w)
		if !strings.Contains(r.URL.Path, "_bulk") {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write([]byte(`{"errors":true,"items":[` +
			`{"index":{"_index":"idx","_id":"1","status":400,"error":{"reason":"bad mapping"}}}]}`))
	}))
	defer srv.Close()

	handler := &recordingHandler{}
	b := buildBulk(esClientForServer(t, srv.URL), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err == nil {
		t.Fatal("expected error for terminal failure")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("terminal failure must not retry: expected 1 call, got %d", got)
	}
	if len(handler.errored) != 1 || len(handler.success) != 0 {
		t.Fatalf("expected 1 errored / 0 success, got %v / %v", handler.errored, handler.success)
	}
}

// A malformed response reporting more item errors than were submitted must not
// panic on pending[ie.position]; the out-of-range item is ignored and the real
// one is still finalized.
func Test_requestFuncWithRetry_MoreItemsThanSubmitted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elasticProduct(w)
		if !strings.Contains(r.URL.Path, "_bulk") {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		// One item submitted, two item errors returned (position 1 is phantom).
		_, _ = w.Write([]byte(`{"errors":true,"items":[` +
			`{"index":{"_index":"idx","_id":"1","status":400,"error":{"reason":"bad"}}},` +
			`{"index":{"_index":"idx","_id":"x","status":400,"error":{"reason":"phantom"}}}]}`))
	}))
	defer srv.Close()

	handler := &recordingHandler{}
	b := buildBulk(esClientForServer(t, srv.URL), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err == nil {
		t.Fatal("expected error for the terminal item")
	}
	if len(handler.errored) != 1 || handler.errored[0] != "1" {
		t.Fatalf("expected only submitted item 1 errored, got %v", handler.errored)
	}
}

func Test_requestFuncWithRetry_ExhaustsRetries(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		elasticProduct(w)
		if !strings.Contains(r.URL.Path, "_bulk") {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write([]byte(`{"errors":true,"items":[` +
			`{"index":{"_index":"idx","_id":"1","status":503,"error":{"reason":"unavailable"}}}]}`))
	}))
	defer srv.Close()

	handler := &recordingHandler{}
	b := buildBulk(esClientForServer(t, srv.URL), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	// maxRetries=2 -> attempts 0,1,2 == 3 calls
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 calls (initial + 2 retries), got %d", got)
	}
	if len(handler.errored) != 1 {
		t.Fatalf("expected 1 errored item, got %v", handler.errored)
	}
}

// stubTransport lets a test drive exact per-call bulk responses (including
// transport errors and whole-response statuses) that an httptest server can't
// reliably produce.
type stubTransport struct {
	responder func(call int) (*http.Response, error)
	mu        sync.Mutex
	bulkCalls int
}

func (s *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.Contains(req.URL.Path, "_bulk") {
		return jsonResp(200, `{}`), nil
	}
	s.mu.Lock()
	s.bulkCalls++
	call := s.bulkCalls
	s.mu.Unlock()
	return s.responder(call)
}

func (s *stubTransport) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bulkCalls
}

func jsonResp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header: http.Header{
			"X-Elastic-Product": {"Elasticsearch"},
			"Content-Type":      {"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

// esClientWithTransport builds a client whose transport is fully stubbed, with
// the client's own retry disabled so error/status handling is driven entirely
// by our retry loop.
func esClientWithTransport(t *testing.T, rt http.RoundTripper) *esv7.Client {
	t.Helper()
	c, err := esv7.NewClient(esv7.Config{
		Addresses:    []string{"http://localhost:9200"},
		Transport:    rt,
		DisableRetry: true,
	})
	if err != nil {
		t.Fatalf("build es client: %v", err)
	}
	return c
}

func Test_bulkRequestPartition_DispatchesToRetryWhenEnabled(t *testing.T) {
	st := &stubTransport{responder: func(call int) (*http.Response, error) {
		if call == 1 {
			return jsonResp(200, `{"errors":true,"items":[`+
				`{"index":{"_index":"idx","_id":"1","status":429,"error":{"reason":"busy"}}}]}`), nil
		}
		return jsonResp(200, `{"errors":false}`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	err := b.bulkRequestPartition(
		[]*elasticsearch.BatchItem{indexItem("1")},
		b.esClients[""],
		config.Elasticsearch{Retry: fastRetry()},
	)
	if err != nil {
		t.Fatalf("enabled retry must recover, got %v", err)
	}
	if st.calls() != 2 {
		t.Fatalf("expected 2 bulk calls via retry path, got %d", st.calls())
	}
}

func Test_bulkRequestPartition_UsesLegacyWhenDisabled(t *testing.T) {
	st := &stubTransport{responder: func(_ int) (*http.Response, error) {
		return jsonResp(200, `{"errors":true,"items":[`+
			`{"index":{"_index":"idx","_id":"1","status":429,"error":{"reason":"busy"}}}]}`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	// Retry nil -> legacy requestFunc, which does not retry per-item failures.
	err := b.bulkRequestPartition(
		[]*elasticsearch.BatchItem{indexItem("1")},
		b.esClients[""],
		config.Elasticsearch{MaxRetries: 1},
	)
	if err == nil {
		t.Fatal("legacy path must surface the bulk error")
	}
	if st.calls() != 1 {
		t.Fatalf("legacy path must not retry per-item failures: got %d calls", st.calls())
	}
}

func Test_requestFuncWithRetry_WholeResponseRetryableStatus(t *testing.T) {
	st := &stubTransport{responder: func(call int) (*http.Response, error) {
		if call == 1 {
			return jsonResp(429, `{"error":"too many requests"}`), nil
		}
		return jsonResp(200, `{"errors":false}`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err != nil {
		t.Fatalf("retryable whole-response status must recover, got %v", err)
	}
	if st.calls() != 2 {
		t.Fatalf("expected 2 calls for whole-response 429 retry, got %d", st.calls())
	}
}

func Test_requestFuncWithRetry_TransportErrorRetried(t *testing.T) {
	st := &stubTransport{responder: func(call int) (*http.Response, error) {
		if call == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		return jsonResp(200, `{"errors":false}`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err != nil {
		t.Fatalf("retryable transport error must recover, got %v", err)
	}
	if st.calls() != 2 {
		t.Fatalf("expected 2 calls for transport-error retry, got %d", st.calls())
	}
}

func Test_requestFuncWithRetry_ParseErrorIsTerminal(t *testing.T) {
	st := &stubTransport{responder: func(_ int) (*http.Response, error) {
		return jsonResp(200, `{bad json`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	err := b.requestFuncWithRetry(0, []*elasticsearch.BatchItem{indexItem("1")}, b.esClients[""], fastRetry())()
	if err == nil {
		t.Fatal("unparseable response must be surfaced as error")
	}
	if st.calls() != 1 {
		t.Fatalf("parse error must not retry, got %d calls", st.calls())
	}
	if len(handler.errored) != 1 {
		t.Fatalf("expected 1 errored item, got %v", handler.errored)
	}
}

// Verifies retry-loop position->globalIdx mapping: on the retry only the failed
// subset is re-submitted, so response positions shift and must map back to the
// original batch indexes.
func Test_requestFuncWithRetry_PartialRetryIndexMapping(t *testing.T) {
	st := &stubTransport{responder: func(call int) (*http.Response, error) {
		if call == 1 {
			// items 1 & 3 (positions 0,2) retryable; item 2 (position 1) ok
			return jsonResp(200, `{"errors":true,"items":[`+
				`{"index":{"_index":"idx","_id":"1","status":429,"error":{"reason":"busy"}}},`+
				`{"index":{"_index":"idx","_id":"2","status":201}},`+
				`{"index":{"_index":"idx","_id":"3","status":429,"error":{"reason":"busy"}}}]}`), nil
		}
		// retry carries only ids 1 & 3 at positions 0,1: id 1 ok, id 3 terminal 400
		return jsonResp(200, `{"errors":true,"items":[`+
			`{"index":{"_index":"idx","_id":"1","status":201}},`+
			`{"index":{"_index":"idx","_id":"3","status":400,"error":{"reason":"bad"}}}]}`), nil
	}}
	handler := &recordingHandler{}
	b := buildBulk(esClientWithTransport(t, st), handler)

	err := b.requestFuncWithRetry(0,
		[]*elasticsearch.BatchItem{indexItem("1"), indexItem("2"), indexItem("3")},
		b.esClients[""], fastRetry())()
	if err == nil {
		t.Fatal("expected terminal failure of item 3 to surface")
	}
	if len(handler.errored) != 1 || handler.errored[0] != "3" {
		t.Fatalf("only item 3 must be errored (correct index mapping), got %v", handler.errored)
	}
	if len(handler.success) != 2 {
		t.Fatalf("items 1 and 2 must succeed, got %v", handler.success)
	}
}

func Test_parseBulkItemErrors_ErrorCases(t *testing.T) {
	if _, err := parseBulkItemErrors(nil); err == nil {
		t.Fatal("nil response must return an error")
	}
	if _, err := parseBulkItemErrors(newResponse(`{bad json`)); err == nil {
		t.Fatal("invalid JSON must return an error")
	}
}

func clusterItem(id, clusterKey string) *elasticsearch.BatchItem {
	return &elasticsearch.BatchItem{
		Action: &document.ESActionDocument{ID: []byte(id), IndexName: "idx", Type: document.Index, ClusterKey: clusterKey},
		Bytes:  []byte(`{"index":{"_index":"idx","_id":"` + id + `"}}` + "\n" + `{"v":1}` + "\n"),
	}
}

func newMetric() *Metric {
	return &Metric{
		IndexingSuccessActionCounter: map[string]int64{},
		IndexingErrorActionCounter:   map[string]int64{},
		DeletionSuccessActionCounter: map[string]int64{},
		DeletionErrorActionCounter:   map[string]int64{},
	}
}

// Test_bulkRequest_PerClusterRetryIsolation covers the multi-cluster fan-out:
// an unhealthy default cluster exhausts its retries and fails, while the
// analytics cluster (routed via ClusterKey) is unaffected and succeeds on the
// first try. Also exercises per-cluster retry settings resolution.
func Test_bulkRequest_PerClusterRetryIsolation(t *testing.T) {
	defaultST := &stubTransport{responder: func(_ int) (*http.Response, error) {
		return jsonResp(200, `{"errors":true,"items":[`+
			`{"index":{"_index":"idx","_id":"d","status":503,"error":{"reason":"unavailable"}}}]}`), nil
	}}
	analyticsST := &stubTransport{responder: func(_ int) (*http.Response, error) {
		return jsonResp(200, `{"errors":false}`), nil
	}}

	handler := &recordingHandler{}
	b := &Bulk{
		concurrentRequest:   1,
		readers:             []*helper.MultiDimByteReader{helper.NewMultiDimByteReader(nil)},
		metric:              newMetric(),
		sinkResponseHandler: handler,
		esClients: map[string]*esv7.Client{
			"":          esClientWithTransport(t, defaultST),
			"analytics": esClientWithTransport(t, analyticsST),
		},
		config: &config.Config{Elasticsearch: config.Elasticsearch{
			Retry: fastRetry(),
			Clusters: map[string]config.Elasticsearch{
				"analytics": {Retry: fastRetry()},
			},
		}},
		batch: []*elasticsearch.BatchItem{
			clusterItem("d", ""),
			clusterItem("a", "analytics"),
		},
	}

	err := b.bulkRequest()
	if err == nil {
		t.Fatal("default cluster failure must surface")
	}
	if defaultST.calls() != 3 {
		t.Fatalf("default cluster must exhaust retries (3 calls), got %d", defaultST.calls())
	}
	if analyticsST.calls() != 1 {
		t.Fatalf("analytics cluster must be unaffected (1 call), got %d", analyticsST.calls())
	}
	if len(handler.errored) != 1 || handler.errored[0] != "d" {
		t.Fatalf("only default-cluster item must error, got %v", handler.errored)
	}
	if len(handler.success) != 1 || handler.success[0] != "a" {
		t.Fatalf("analytics item must succeed, got %v", handler.success)
	}
}
