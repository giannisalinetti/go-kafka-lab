package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	serverName := flag.String("server", "localhost", "Server name/address")
	serverPort := flag.String("port", "localhost", "Server port")
	topicName := flag.String("topic", "test", "Topic name")
	flag.Parse()

	config := &kafka.ConfigMap{
		"bootstrap.servers": *serverName + ":" + *serverPort,
		"group.id":          "test-group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Error creating consumer: %s\n", err)
	}

	sigChain := make(chan os.Signal, 1)
	signal.Notify(sigChain, syscall.SIGINT, syscall.SIGTERM)

	consumer.SubscribeTopics([]string{*topicName}, nil)
	for {
		select {
		case <-sigChain:
			log.Println("Shutting down")
			consumer.Close()
			os.Exit(0)
		default:
			event := consumer.Poll(100)
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				log.Printf("Received message with value: %s\n", e.Value)
			case kafka.OffsetsCommitted:
				log.Printf("Offsets committed: %v\n", e)
			}
		}
	}
}
