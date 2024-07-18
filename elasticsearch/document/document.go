package document

type EsAction string

const (
	Index  EsAction = "Index"
	Delete EsAction = "Delete"
)

type ESActionDocument struct {
	Routing   *string
	Type      EsAction
	IndexName string
	Source    []byte
	ID        []byte
}

func NewDeleteAction(key []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:      key,
		Routing: routing,
		Type:    Delete,
	}
}

func NewDeleteActionWithIndexName(indexName string, key []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:        key,
		Routing:   routing,
		Type:      Delete,
		IndexName: indexName,
	}
}

func NewIndexAction(key []byte, source []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:      key,
		Routing: routing,
		Source:  source,
		Type:    Index,
	}
}

func NewIndexActionWithIndexName(indexName string, key []byte, source []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:        key,
		Routing:   routing,
		Source:    source,
		Type:      Index,
		IndexName: indexName,
	}
}
