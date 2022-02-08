package sdk

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"time"
)

var (
	DefaultNatsReconnectWait =  time.Second
	DefaultNatsMaxConnectsIfErr = 86400
)

//ServiceMQ .
type serviceMQ struct {
	Conn *nats.Conn
}

type ConsumerMsg struct {
	Subject string
	Data    []byte
}

var srvMQ *serviceMQ

// NewServiceMQ .
func NewServiceMQ(addr string) *serviceMQ {
	if srvMQ != nil {
		return srvMQ
	}
	nc, err := ConnNats(addr)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if nc == nil {
		return nil
	}
	srvMQ = &serviceMQ{}
	srvMQ.Conn = nc
	return srvMQ
}

func ConnNats(addr string) (*nats.Conn, error) {
	nc, err := nats.Connect(addr, nats.ReconnectWait(DefaultNatsReconnectWait), nats.MaxReconnects(DefaultNatsMaxConnectsIfErr))
	if nc == nil {
		return nil, errors.Errorf("connect to nats error, because conn is nil")
	}
	if err != nil {
		return nil, errors.Wrap(err, "connect to nats error")
	}
	return nc, nil
}

// Send message
func (s *serviceMQ) Send(topic, message string) error {
	if s.Conn == nil {
		return fmt.Errorf("get nats conn error")
	}
	status := s.Conn.Status()
	if status == nats.CONNECTED {
		if err := s.Conn.Publish(topic, []byte(message)); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("conn error, status: [%v]", status)
	}
	return nil
}

// AddListener listen some topics
func (s *serviceMQ) AddListener(topics ...string) (<-chan *ConsumerMsg, error) {
	if s.Conn == nil {
		return nil, fmt.Errorf("listen topic :%v error", topics)
	}

	ch := make(chan *ConsumerMsg, 10000)
	for _, v := range topics {
		if _, err := s.Conn.Subscribe(v, func(m *nats.Msg) {
			msg := &ConsumerMsg{
				Subject: m.Subject,
				Data:    m.Data,
			}
			ch <- msg
		}); err != nil {
			fmt.Println(err)
		}
	}
	return ch, nil
}

