package consumer

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

//Config Configuration to connect to kafka for reading messages
func Config() kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
	}
}

//Consume Consumes all messages
func Consume() {
	fmt.Println("Start consuming ...")
	kafkaConsumerConfig := Config()
	c, err := kafka.NewConsumer(&kafkaConsumerConfig)

	if err != nil {
		panic(err)
	}
	//Subscribes to the topic who receive publisher messages
	c.SubscribeTopics([]string{"tipicTopic"}, nil)

	//Receives the message. -1 is timeout: indefinite timeout
	msg, err := c.ReadMessage(-1)

	if err == nil {
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	} else {
		fmt.Printf("Consumer error: %v (%v)\n", err, msg)
	}

	c.Close()
}
