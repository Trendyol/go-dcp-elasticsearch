package dcpelasticsearch

import (
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/bulk"
)

type DcpEventHandler struct {
	bulk     *bulk.Bulk
	isFinite bool
}

func (h *DcpEventHandler) BeforeRebalanceStart() {
}

func (h *DcpEventHandler) AfterRebalanceStart() {
}

func (h *DcpEventHandler) BeforeRebalanceEnd() {
}

func (h *DcpEventHandler) AfterRebalanceEnd() {
}

func (h *DcpEventHandler) BeforeStreamStart() {
	h.bulk.PrepareEndRebalancing()
}

func (h *DcpEventHandler) AfterStreamStart() {
}

func (h *DcpEventHandler) BeforeStreamStop() {
	if h.isFinite {
		return
	}
	h.bulk.PrepareStartRebalancing()
}

func (h *DcpEventHandler) AfterStreamStop() {
}
