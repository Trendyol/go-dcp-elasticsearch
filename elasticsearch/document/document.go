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

func NewIndexAction(key []byte, source []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:      key,
		Routing: routing,
		Source:  source,
		Type:    Index,
	}
}
