package main

import (
	consumer "go-kafka/consumer"
	producer "go-kafka/producer"
)

/*
Starts producer, produces some messages and closes the producer connection
Starts the consumer, and consumes the messages already produced.
*/
func main() {

	producer.Produce()

	consumer.Consume()

}
