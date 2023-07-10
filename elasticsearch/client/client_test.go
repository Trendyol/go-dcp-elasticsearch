package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/elastic/go-elasticsearch/v7/esapi"
)

// nolint:revive
func TestHasResponseError(t *testing.T) {
	// given
	testCases := []struct {
		name     string
		response *esapi.Response
		expected error
	}{
		{
			name: "NoError",
			response: &esapi.Response{
				StatusCode: http.StatusOK,
				Header:     nil,
				Body:       io.NopCloser(bytes.NewBufferString(`{"errors": false}`)),
			},
			expected: nil,
		},
		{
			name: "HasError",
			response: &esapi.Response{
				StatusCode: http.StatusOK,
				Header:     nil,
				Body: io.NopCloser(bytes.NewBufferString(`{"errors": true, "items": [
			{"error": "error 1"},
			{"key": "value"},
			{"error": "error 2"}
		]}`)),
			},
			expected: fmt.Errorf("bulk request has error. Errors will be listed below:\n"),
		},
	}

	// when & than
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := hasResponseError(tc.response)
			if !reflect.DeepEqual(err, tc.expected) {
				t.Errorf("hasResponseError() = %v, expected %v", err, tc.expected)
			}
		})
	}
}

func TestJoinErrors(t *testing.T) {
	// given
	testCases := []struct {
		name     string
		body     map[string]interface{}
		expected error
	}{
		{
			name:     "NoErrors",
			body:     map[string]interface{}{},
			expected: nil,
		},
		{
			name: "HasErrors",
			body: map[string]any{
				"items": []any{
					map[string]any{
						"error": map[string]any{"error": "error 1"},
					},
					map[string]any{
						"key": "value",
					},
					map[string]any{
						"error": map[string]any{"error": "error 2"},
					},
				},
			},
			expected: fmt.Errorf("bulk request has error. Errors will be listed below:\n" +
				"map[error:map[error:error 1]]\n" +
				"map[error:map[error:error 2]]\n"),
		},
	}

	// when & than
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := joinErrors(tc.body)
			if !reflect.DeepEqual(err, tc.expected) {
				t.Errorf("joinErrors() = %v, expected %v", err, tc.expected)
			}
		})
	}
}
