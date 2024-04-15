package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"log"
	"strings"
	"time"
)

type (
	asyncProducerService struct {
		name    string
		context context.Context
		client  sarama.AsyncProducer
		cancel  func()
	}

	syncProducerService struct {
		name    string
		context context.Context
		client  sarama.SyncProducer
		cancel  func()
	}
)

func NewProducer(ctx context.Context, conf ProducerConfig) (Producer, error) {
	config := sarama.NewConfig()
	version, err := conf.parseVersion()
	if err != nil {
		config.Version = version
	}

	config.Metadata.Full = true
	config.ChannelBufferSize = 2560

	if conf.Auth.Enable {
		config.Net.SASL.Enable = conf.Auth.Enable
		config.Net.SASL.User = conf.Auth.Username
		config.Net.SASL.Password = conf.Auth.Password

		conf.Auth.Mechanism = SASLMechanism(strings.TrimSpace(string(conf.Auth.Mechanism)))
		if string(conf.Auth.Mechanism) == "" {
			conf.Auth.Mechanism = SCRAMSHA512
		}

		switch conf.Auth.Mechanism {
		case SCRAMSHA256:
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case SCRAMSHA512:
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		default:
			return nil, errors.New(ErrMechanismInvalid)
		}
	}

	if conf.TLS.Enable {
		config.Net.TLS.Enable = conf.TLS.Enable
		config.Net.TLS.Config, err = conf.TLS.createTLSConfiguration()
		if err != nil {
			return nil, err
		}
	}

	conf.Brokers = strings.TrimSpace(conf.Brokers)
	if conf.Brokers == "" {
		return nil, errors.New(ErrBrokerNotFound)
	}

	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Timeout = time.Minute
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = time.Second * 5

	if conf.Async {
		pro, err := sarama.NewAsyncProducer(strings.Split(conf.Brokers, ","), config)
		if err != nil {
			return nil, err
		}
		ps := &asyncProducerService{
			name:    ProducerModule,
			context: nil,
			client:  pro,
			cancel:  nil,
		}
		ps.context, ps.cancel = context.WithCancel(ctx)
		go ps.monitor()
		return ps, nil
	} else {
		pro, err := sarama.NewSyncProducer(strings.Split(conf.Brokers, ","), config)
		if err != nil {
			return nil, err
		}
		ps := &syncProducerService{
			name:    ProducerModule,
			context: nil,
			client:  pro,
			cancel:  nil,
		}
		ps.context, ps.cancel = context.WithCancel(ctx)
		return ps, nil
	}
}

func (p *syncProducerService) Produce(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(value)
	}
	if _, _, err := p.client.SendMessage(msg); err != nil {
		return err
	}
	return nil
}

func (p *syncProducerService) Close() {
	if err := p.client.Close(); err != nil {
		log.Printf("failed to close sync producer, error: %v", err)
	}
}

func (p *asyncProducerService) Produce(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(value)
	}

	p.client.Input() <- msg
	return nil
}

func (p *asyncProducerService) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *asyncProducerService) monitor() {
	for {
		select {
		case err := <-p.client.Errors():
			if err != nil {
				bts, e := err.Msg.Value.Encode()
				if e != nil {
					log.Printf("failed to get error message, error: %v", err)
					continue
				}
				log.Printf("failed to produce message, error: %v, document: %v", err, string(bts))
			}
		case <-p.context.Done():
			log.Printf("close async producer")
			if err := p.client.Close(); err != nil {
				log.Printf("failed to close async producer, error: %v", err)
			}
			return
		}
	}
}
