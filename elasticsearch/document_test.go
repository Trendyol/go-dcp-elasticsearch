package elasticsearch

import (
	"reflect"
	"testing"
)

func TestNewDeleteAction(t *testing.T) {
	key := []byte("key")
	routing := "routing"

	action := NewDeleteAction(key, &routing)

	expectedAction := ActionDocument{
		ID:      key,
		Routing: &routing,
		Type:    Delete,
	}

	if !reflect.DeepEqual(action, expectedAction) {
		t.Errorf("NewDeleteAction() = %v, expected %v", action, expectedAction)
	}
}

func TestNewIndexAction(t *testing.T) {
	key := []byte("key")
	source := []byte("source")
	routing := "routing"

	action := NewIndexAction(key, source, &routing)

	expectedAction := ActionDocument{
		ID:      key,
		Routing: &routing,
		Source:  source,
		Type:    Index,
	}

	if !reflect.DeepEqual(action, expectedAction) {
		t.Errorf("NewIndexAction() = %v, expected %v", action, expectedAction)
	}
}
