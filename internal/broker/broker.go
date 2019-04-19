package broker

// Broker Broker
type Broker interface {
	Init(Options) error
	Publish(message []byte) error
	Subscribe(handler Handler) error
	Close() error
	String() string
	Ping() error
}

// Handler Handler
type Handler func(Publication) error

// Options Options
type Options struct {
	Topic      string
	Group      string // cunsumer group
	Addrs      []string
	AutoAck    bool
	IsCunsumer bool
}

// Publication Publication
type Publication interface {
	Topic() string
	Message() []byte
	Ack() error
}
