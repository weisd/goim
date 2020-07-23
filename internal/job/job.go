package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/Terry-Mao/goim/api/logic/grpc"
	"github.com/Terry-Mao/goim/internal/job/conf"
	"github.com/bilibili/discovery/naming"
	"github.com/gogo/protobuf/proto"

	"github.com/Terry-Mao/goim/internal/broker"

	log "github.com/golang/glog"
)

// Job is push job.
type Job struct {
	c            *conf.Config
	broker       broker.Broker
	cometServers map[string]*Comet

	rooms      map[string]*Room
	roomsMutex sync.RWMutex
}

// New new a push job.
func New(c *conf.Config) *Job {
	j := &Job{
		c:      c,
		broker: newBroker(c),
		rooms:  make(map[string]*Room),
	}
	j.watchComet(c.Discovery)
	return j
}

func newBroker(c *conf.Config) broker.Broker {
	b, ok := broker.Brokers[c.Broker]

	if !ok {
		panic("broker not found")
	}

	b.Init(c.BrokerOption)

	return b
}

// func newKafkaSub(c *conf.Kafka) *cluster.Consumer {
// 	config := cluster.NewConfig()
// 	config.Consumer.Return.Errors = true
// 	config.Group.Return.Notifications = true
// 	consumer, err := cluster.NewConsumer(c.Brokers, c.Group, []string{c.Topic}, config)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return consumer
// }

// Close close resounces.
func (j *Job) Close() error {
	if j.broker != nil {
		return j.broker.Close()
	}
	return nil
}

// Consume messages, watch signals
func (j *Job) Consume() {

	j.broker.Subscribe(func(pub broker.Publication) error {
		// process push message
		pushMsg := new(pb.PushMsg)
		if err := proto.Unmarshal(pub.Message(), pushMsg); err != nil {
			log.Errorf("proto.Unmarshal(%v) error(%v)", pub.Message(), err)
			return err
		}
		if err := j.push(context.Background(), pushMsg); err != nil {
			log.Errorf("j.push(%v) error(%v)", pushMsg, err)
		}
		log.Infof("consume: %s/\t%+v", pub.Topic(), pushMsg)
		return nil
	})
}

func (j *Job) watchComet(c *naming.Config) {
	dis := naming.New(c)
	resolver := dis.Build("goim.comet")
	event := resolver.Watch()
	select {
	case _, ok := <-event:
		if !ok {
			panic("watchComet init failed")
		}
		if ins, ok := resolver.Fetch(); ok {
			if err := j.newAddress(ins.Instances); err != nil {
				panic(err)
			}
			log.Infof("watchComet init newAddress:%+v", ins)
		}
	case <-time.After(10 * time.Second):
		log.Error("watchComet init instances timeout")
	}
	go func() {
		for {
			if _, ok := <-event; !ok {
				log.Info("watchComet exit")
				return
			}
			ins, ok := resolver.Fetch()
			if ok {
				if err := j.newAddress(ins.Instances); err != nil {
					log.Errorf("watchComet newAddress(%+v) error(%+v)", ins, err)
					continue
				}
				log.Infof("watchComet change newAddress:%+v", ins)
			}
		}
	}()
}

func (j *Job) newAddress(insMap map[string][]*naming.Instance) error {
	ins := insMap[j.c.Env.Zone]
	if len(ins) == 0 {
		return fmt.Errorf("watchComet instance is empty")
	}
	comets := map[string]*Comet{}
	for _, in := range ins {
		if old, ok := j.cometServers[in.Hostname]; ok {
			comets[in.Hostname] = old
			continue
		}
		c, err := NewComet(in, j.c.Comet)
		if err != nil {
			log.Errorf("watchComet NewComet(%+v) error(%v)", in, err)
			return err
		}
		comets[in.Hostname] = c
		log.Infof("watchComet AddComet grpc:%+v", in)
	}
	for key, old := range j.cometServers {
		if _, ok := comets[key]; !ok {
			old.cancel()
			log.Infof("watchComet DelComet:%s", key)
		}
	}
	j.cometServers = comets
	return nil
}
