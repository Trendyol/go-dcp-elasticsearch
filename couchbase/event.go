package couchbase

type Event struct {
	CollectionName string
	Key            []byte
	Value          []byte
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
}

func NewDeleteEvent(key []byte, value []byte, collectionName string) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
	}
}

func NewExpireEvent(key []byte, value []byte, collectionName string) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
	}
}

func NewMutateEvent(key []byte, value []byte, collectionName string) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
	}
}
