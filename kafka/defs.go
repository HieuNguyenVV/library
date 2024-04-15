package kafka

const (
	ConsumerModule = "kafka-consumer"
	ProducerModule = "kafka-producer"

	SCRAMSHA256 SASLMechanism = "sha256"
	SCRAMSHA512 SASLMechanism = "sha512"

	OffsetOldest Offset = "oldest"
	OffsetNewest Offset = "newest"
)
