package elasticsearch

import (
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
)

type BatchItem struct {
	Action *document.ESActionDocument
	Bytes  []byte
}
