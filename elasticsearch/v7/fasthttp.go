package v7

import (
	"io"
	"net/http"
	"strings"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase/config"

	"github.com/valyala/fasthttp"
)

// transport implements the elastictransport interface with
// the github.com/valyala/fasthttp HTTP client.
type transport struct {
	client *fasthttp.Client
}

func newTransport(cfg config.Elasticsearch) *transport {
	client := &fasthttp.Client{
		MaxConnsPerHost:     fasthttp.DefaultMaxConnsPerHost,
		MaxIdleConnDuration: fasthttp.DefaultMaxIdleConnDuration,
	}

	if cfg.MaxConnsPerHost != nil {
		client.MaxConnsPerHost = *cfg.MaxConnsPerHost
	}

	if cfg.MaxIdleConnDuration != nil {
		client.MaxIdleConnDuration = *cfg.MaxIdleConnDuration
	}

	return &transport{client: client}
}

// RoundTrip performs the request and returns a response or error
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	freq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(freq)

	fres := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(fres)

	t.copyRequest(freq, req)

	err := t.client.Do(freq, fres)
	if err != nil {
		return nil, err
	}

	res := &http.Response{Header: make(http.Header)}
	t.copyResponse(res, fres)

	return res, nil
}

// copyRequest converts a http.Request to fasthttp.Request
func (t *transport) copyRequest(dst *fasthttp.Request, src *http.Request) *fasthttp.Request {
	if src.Method == http.MethodGet && src.Body != nil {
		src.Method = http.MethodPost
	}
	dst.SetHost(src.Host)
	dst.SetRequestURI(src.URL.String())
	dst.Header.SetRequestURI(src.URL.String())
	dst.Header.SetMethod(src.Method)

	for k, vv := range src.Header {
		for _, v := range vv {
			dst.Header.Set(k, v)
		}
	}

	if src.Body != nil {
		dst.SetBodyStream(src.Body, -1)
	}

	return dst
}

// copyResponse converts a http.Response to fasthttp.Response
func (t *transport) copyResponse(dst *http.Response, src *fasthttp.Response) *http.Response {
	dst.StatusCode = src.StatusCode()

	src.Header.VisitAll(func(k, v []byte) {
		dst.Header.Set(string(k), string(v))
	})

	// Cast to a string to make a copy seeing as src.Body() won't
	// be valid after the response is released back to the pool (fasthttp.ReleaseResponse).
	dst.Body = io.NopCloser(strings.NewReader(string(src.Body())))

	return dst
}
