package broker

// Brokers Brokers
var Brokers = map[string]Broker{}

// Register Register
func Register(b Broker)  {
	Brokers[b.String()] = b
}