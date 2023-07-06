package mock

func NewMockListenerContext() *ListenerContext {
	return &ListenerContext{}
}

type ListenerContext struct {
	CommitCalled bool
	AckCalled    bool
}

func (m *ListenerContext) Commit() {
	m.CommitCalled = true
}

func (m *ListenerContext) Ack() {
	m.AckCalled = true
}
