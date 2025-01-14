package document

import "fmt"

type EsAction string

const (
	Index        EsAction = "Index"
	Delete       EsAction = "Delete"
	DocUpdate    EsAction = "DocUpdate"
	ScriptUpdate EsAction = "ScriptUpdate"
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

func NewDocUpdateAction(key []byte, source []byte, routing *string, partialIndexObjectName string) ESActionDocument {
	return ESActionDocument{
		ID:      key,
		Routing: routing,
		Source:  []byte(fmt.Sprintf(`{"%s":%s}`, partialIndexObjectName, source)),
		Type:    DocUpdate,
	}
}

func NewDocUpdateActionWithIndexName(
	key []byte,
	source []byte,
	routing *string,
	indexName string,
	partialIndexObjectName string,
) ESActionDocument {
	return ESActionDocument{
		ID:        key,
		Routing:   routing,
		Source:    []byte(fmt.Sprintf(`{"%s":%s}`, partialIndexObjectName, source)),
		Type:      DocUpdate,
		IndexName: indexName,
	}
}

func NewScriptUpdateAction(id []byte, script []byte, routing *string) ESActionDocument {
	return ESActionDocument{
		ID:      id,
		Type:    ScriptUpdate,
		Source:  script,
		Routing: routing,
	}
}

func NewScriptUpdateActionWithIndexName(id []byte, script []byte, routing *string, indexName string) ESActionDocument {
	return ESActionDocument{
		ID:        id,
		Type:      ScriptUpdate,
		Source:    script,
		Routing:   routing,
		IndexName: indexName,
	}
}
