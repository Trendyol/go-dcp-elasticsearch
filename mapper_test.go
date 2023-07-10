package goelasticsearchconnectcouchbase_test

import (
	"testing"
	"time"

	"github.com/Trendyol/go-elasticsearch-connect-couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/couchbase"
	"github.com/Trendyol/go-elasticsearch-connect-couchbase/elasticsearch"
)

func TestDefaultMapper(t *testing.T) {
	// given
	tests := []struct {
		name  string
		event couchbase.Event
		want  []elasticsearch.ActionDocument
	}{
		{
			name: "Mutated Event",
			event: couchbase.Event{
				CollectionName: "test-collection",
				EventTime:      time.Now(),
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
				IsDeleted:      false,
				IsExpired:      false,
				IsMutated:      true,
			},
			want: []elasticsearch.ActionDocument{
				{
					Type:   elasticsearch.Index,
					Source: []byte("test-value"),
					ID:     []byte("test-key"),
				},
			},
		},
		{
			name: "Deleted Event",
			event: couchbase.Event{
				CollectionName: "test-collection",
				EventTime:      time.Now(),
				Key:            []byte("test-key"),
				Value:          []byte("test-value"),
				IsDeleted:      true,
				IsExpired:      false,
				IsMutated:      false,
			},
			want: []elasticsearch.ActionDocument{
				{
					Type: elasticsearch.Delete,
					ID:   []byte("test-key"),
				},
			},
		},
	}

	// when & then
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := goelasticsearchconnectcouchbase.DefaultMapper(tt.event)
			if len(got) != len(tt.want) {
				t.Errorf("got %d actions, want %d actions", len(got), len(tt.want))
			}

			for i := range got {
				if got[i].Type != tt.want[i].Type {
					t.Errorf("action %d type mismatch, got %s, want %s", i, got[i].Type, tt.want[i].Type)
				}
				if string(got[i].Source) != string(tt.want[i].Source) {
					t.Errorf("action %d source mismatch, got %s, want %s", i, got[i].Source, tt.want[i].Source)
				}
				if string(got[i].ID) != string(tt.want[i].ID) {
					t.Errorf("action %d ID mismatch, got %s, want %s", i, got[i].ID, tt.want[i].ID)
				}
			}
		})
	}
}
