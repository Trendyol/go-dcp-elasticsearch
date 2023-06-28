package elasticsearch

type Action string

const (
	Index  Action = "Index"
	Delete Action = "Delete"
)

type ActionDocument struct {
	Routing *string
	Type    Action
	Source  []byte
	ID      []byte
}

func NewDeleteAction(key []byte, routing *string) ActionDocument {
	return ActionDocument{
		ID:      key,
		Routing: routing,
		Type:    Delete,
	}
}

func NewIndexAction(key []byte, source []byte, routing *string) ActionDocument {
	return ActionDocument{
		ID:      key,
		Routing: routing,
		Source:  source,
		Type:    Index,
	}
}
