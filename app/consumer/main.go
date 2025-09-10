package main

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/daniilt23/app/models"
)

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "orders",
		Balancer: &kafka.LeastBytes{},
	})

	defer w.Close()

	for {
		order := generateFakeOrder()

		sendMessage(w, order)

		time.Sleep(time.Second * 3)
	}
}

func generateFakeOrder() Order {

}