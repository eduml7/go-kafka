package producer

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

//Config Configuration to connect to kafka for producing messages
func Config() kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": "localhost",
	}
}

//Produce produces messages
func Produce() {

	fmt.Println("Start producing ...")
	kafkaConfig := Config()
	kafkaProducer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		panic(err)
	}

	//CLoses connection when the function returns
	defer kafkaProducer.Close()

	// Delivery report handler: channel who notifies if a message has been delivered
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "tipicTopic"
	for _, word := range []string{"{message: Hello}"} {
		kafkaProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	//This line waits until kafka reports the message has been delivered. Is necessary to
	//see the message status in the delivery report handler
	kafkaProducer.Flush(2000)
	fmt.Println("Closing producer")
}
