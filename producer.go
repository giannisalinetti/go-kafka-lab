package main

import (
	"flag"
	"log"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	serverName := flag.String("server", "localhost", "Server name/address")
	serverPort := flag.String("port", "9092", "Server port")
	topicName := flag.String("topic", "", "Topic name")
	flag.Parse()

	config := &kafka.ConfigMap{
		"bootstrap.servers": *serverName + ":" + *serverPort,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Error creating producer: %s\n", err)
	}

	topic := *topicName
	record := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("Hello World!"),
	}

	producer.ProduceChannel() <- record
	defer producer.Close()

	event := <-producer.Events()
	msg := event.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		log.Printf("Error sending message to cluster: %s\n", msg.TopicPartition.Error)
	} else {
		log.Printf("Message sent to topic %s (partition %d) at offset %d\n",
			*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}
}
