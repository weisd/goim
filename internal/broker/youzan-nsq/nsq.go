package nsq

import (
	"github.com/Terry-Mao/goim/internal/broker"
	"github.com/spaolacci/murmur3"
	nsq "github.com/youzan/go-nsq"
)

func init() {
	broker.Register(NewBroker())
}

// Broker Broker
type Broker struct {
	opt      broker.Options
	producer *nsq.TopicProducerMgr
	consumer *nsq.Consumer
}

// NewBroker NewBroker
func NewBroker() broker.Broker {
	return &Broker{}
}

// Init Init
func (p *Broker) Init(opt broker.Options) error {
	p.opt = opt

	// Publish Publish

	if p.opt.IsCunsumer {
		config := nsq.NewConfig()
		// 启用顺序消费
		config.EnableOrdered = true
		config.Hasher = murmur3.New32()

		consumer, err := nsq.NewConsumer(p.opt.Topic, p.opt.Group, config)
		if err != nil {
			return err
		}

		p.consumer = consumer

		return nil
	}

	config := nsq.NewConfig()

	config.EnableOrdered = true

	config.Hasher = murmur3.New32()

	pubMgr, err := nsq.NewTopicProducerMgr([]string{p.opt.Topic}, config)
	if err != nil {

		return err
	}

	pubMgr.AddLookupdNodes(p.opt.Addrs)

	p.producer = pubMgr

	return nil
}

// Publish Publish
func (p *Broker) Publish(message []byte) error {
	_, _, _, err := p.producer.PublishOrdered(p.opt.Topic, []byte{'1'}, message)
	// err := p.producer.Publish(p.opt.Topic, message)

	return err
}

// Subscribe Subscribe
func (p *Broker) Subscribe(handler broker.Handler) error {

	p.consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		// handle the message
		if err := handler(&publication{
			t:  p.opt.Topic,
			c:  p.consumer,
			cm: m,
		}); err == nil && p.opt.AutoAck {
			m.Finish()
		}
		return nil
	}))

	// 连接lookup地址开始消费

	err := p.consumer.ConnectToNSQLookupds(p.opt.Addrs)
	if err != nil {
		return err
	}

	<-p.consumer.StopChan

	return nil
}

// Close Close
func (p *Broker) Close() error {
	if p.consumer != nil {
		p.consumer.Stop()
	}

	if p.producer != nil {
		p.producer.Stop()
	}

	return nil
}

// String String
func (p *Broker) String() string {
	return "youzan-nsq"
}

// Ping Ping
func (p *Broker) Ping() error {
	return nil
}

type publication struct {
	c  *nsq.Consumer
	t  string
	cm *nsq.Message
}

func (p *publication) Topic() string {
	return p.t
}
func (p *publication) Message() []byte {
	return p.cm.Body
}
func (p *publication) Ack() error {
	p.cm.Finish()
	return nil
}
