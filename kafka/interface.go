package kafka

type (
	Consumer interface {
		Close()
	}

	Producer interface {
		Produce(topic, key string, value []byte) error
		Close()
	}
)
