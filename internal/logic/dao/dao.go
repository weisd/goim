package dao

import (
	"context"
	"time"

	"github.com/Terry-Mao/goim/internal/broker"
	"github.com/Terry-Mao/goim/internal/logic/conf"
	"github.com/gomodule/redigo/redis"
	// kafka "gopkg.in/Shopify/sarama.v1"
)

// Dao dao.
type Dao struct {
	c *conf.Config
	// kafkaPub    kafka.SyncProducer
	broker      broker.Broker
	redis       *redis.Pool
	redisExpire int32
}

// New new a dao and return.
func New(c *conf.Config) *Dao {

	d := &Dao{
		c: c,
		// kafkaPub:    newKafkaPub(c.Kafka),
		broker:      newBroker(c),
		redis:       newRedis(c.Redis),
		redisExpire: int32(time.Duration(c.Redis.Expire) / time.Second),
	}
	return d
}

func newBroker(c *conf.Config) broker.Broker {
	b, ok := broker.Brokers[c.Broker]

	if !ok {
		panic("broker not found")
	}

	b.Init(c.BrokerOption)

	return b
}

// func newKafkaPub(c *conf.Kafka) kafka.SyncProducer {
// 	var err error
// 	kc := kafka.NewConfig()
// 	kc.Producer.RequiredAcks = kafka.WaitForAll // Wait for all in-sync replicas to ack the message
// 	kc.Producer.Retry.Max = 10                  // Retry up to 10 times to produce the message
// 	kc.Producer.Return.Successes = true
// 	pub, err := kafka.NewSyncProducer(c.Brokers, kc)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return pub
// }

func newRedis(c *conf.Redis) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.Idle,
		MaxActive:   c.Active,
		IdleTimeout: time.Duration(c.IdleTimeout),
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(c.Network, c.Addr,
				redis.DialConnectTimeout(time.Duration(c.DialTimeout)),
				redis.DialReadTimeout(time.Duration(c.ReadTimeout)),
				redis.DialWriteTimeout(time.Duration(c.WriteTimeout)),
				redis.DialPassword(c.Auth),
			)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
}

// Close close the resource.
func (d *Dao) Close() error {
	if err := d.redis.Close(); err != nil {
		return err
	}
	return d.broker.Close()
}

// Ping dao ping.
func (d *Dao) Ping(c context.Context) error {
	if err := d.pingRedis(c); err != nil {
		return err
	}

	return d.broker.Ping()
}
