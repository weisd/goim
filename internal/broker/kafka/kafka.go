package kafka

import (
	"context"
	sarama "github.com/Shopify/sarama"
	"github.com/Terry-Mao/goim/internal/broker"
	cluster "github.com/bsm/sarama-cluster"
)

var (
	_ broker.Broker = NewBroker()
)

func init() {
	broker.Register(NewBroker())
}

// Broker Broker
type Broker struct {
	opt      broker.Options
	producer sarama.SyncProducer
	consumer *cluster.Consumer
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewBroker NewBroker
func NewBroker() broker.Broker {
	return &Broker{}
}

// Init Init
func (p *Broker) Init(o broker.Options) error {

	p.opt = o

	ctx, cancel := context.WithCancel(context.Background())

	p.ctx = ctx
	p.cancel = cancel

	return p.Connect()
}

// Connect Connect
func (p *Broker) Connect() error {
	if p.opt.IsCunsumer {
		if err := p.initConsumer(); err != nil {
			return err
		}
		return nil
	}

	if err := p.initProducer(); err != nil {
		return err
	}

	return nil
}

func (p *Broker) initProducer() error {
	if p.producer == nil {
		var err error
		kc := sarama.NewConfig()
		kc.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
		kc.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
		kc.Producer.Return.Successes = true
		pub, err := sarama.NewSyncProducer(p.opt.Addrs, kc)
		if err != nil {
			return err
		}

		p.producer = pub
	}

	return nil
}

func (p *Broker) initConsumer() error {
	if p.consumer == nil {
		config := cluster.NewConfig()
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
		consumer, err := cluster.NewConsumer(p.opt.Addrs, p.opt.Group, []string{p.opt.Topic}, config)
		if err != nil {
			return err
		}

		p.consumer = consumer
	}

	return nil

}

// Publish Publish
func (p *Broker) Publish(message []byte) error {

	m := &sarama.ProducerMessage{
		// Key:   sarama.StringEncoder(room),
		Topic: p.opt.Topic,
		Value: sarama.ByteEncoder(message),
	}

	if _, _, err := p.producer.SendMessage(m); err != nil {
		return err
	}

	return nil
}

// Subscribe Subscribe
func (p *Broker) Subscribe(handler broker.Handler) error {

	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case _ = <-p.consumer.Errors():
			// log.Errorf("consumer error(%v)", err)
		case _ = <-p.consumer.Notifications():
			// log.Infof("consumer rebalanced(%v)", n)
		case msg, ok := <-p.consumer.Messages():
			if !ok {
				return nil
			}

			if err := handler(&publication{
				t:  p.opt.Topic,
				c:  p.consumer,
				cm: msg,
			}); err == nil && p.opt.AutoAck {
				p.consumer.MarkOffset(msg, "")
			}

		}
	}

}

// Close Close
func (p *Broker) Close() error {
	if p.consumer != nil {
		p.consumer.Close()
	}

	if p.producer != nil {
		p.producer.Close()
	}

	p.cancel()

	return nil
}

// Ping health check
func (p *Broker) Ping() error {
	return nil
}

// String String
func (p *Broker) String() string {
	return "kafka"
}

type publication struct {
	c  *cluster.Consumer
	t  string
	cm *sarama.ConsumerMessage
}

func (p *publication) Topic() string {
	return p.t
}
func (p *publication) Message() []byte {
	return p.cm.Value
}
func (p *publication) Ack() error {
	p.c.MarkOffset(p.cm, "")
	return nil
}
