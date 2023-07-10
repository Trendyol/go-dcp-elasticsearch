package couchbase

import (
	"reflect"
	"testing"
)

func TestNewDeleteEvent(t *testing.T) {
	// given
	key := []byte("test-key")
	value := []byte("test-value")
	var collectionName = "test-collection"

	want := Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
	}

	// when
	got := NewDeleteEvent(key, value, collectionName)

	// then
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestNewExpireEvent(t *testing.T) {
	// given
	key := []byte("test-key")
	value := []byte("test-value")
	collectionName := "test-collection"

	want := Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
	}

	// when
	got := NewExpireEvent(key, value, collectionName)

	// then
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestNewMutateEvent(t *testing.T) {
	// given
	key := []byte("test-key")
	value := []byte("test-value")
	collectionName := "test-collection"

	want := Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
	}

	// when
	got := NewMutateEvent(key, value, collectionName)

	// then
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
