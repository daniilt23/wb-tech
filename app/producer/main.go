package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/daniilt23/wb-tech/app/models"
	"github.com/segmentio/kafka-go"
)

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		Balancer: &kafka.LeastBytes{},
	})

	defer w.Close()

	for {
		order := generateFakeOrder()

		sendMessage(w, order)

		time.Sleep(time.Second * 3)
	}
}

func generateFakeOrder() models.Order {
	order_uid := fmt.Sprintf("%d", rand.IntN(10000))

	return models.Order{
		OrderUID:    order_uid,
		TrackNumber: "WBILMTSETTRACK",
		Entry:       "WBIL",
		Delivery: models.Delivery{
			Name:    "Test Testovich",
			Phone:   "+98005553535",
			Zip:     "427256",
			City:    "Keremet",
			Address: "Fake street 23",
			Region:  "Fake region",
			Email:   "test@mail.com",
		},
		Payment: models.Payment{
			OrderUID:     order_uid,
			Transaction:  "hgdjghdjghd53hf75",
			RequestID:    "",
			Currency:     "BTC",
			Provider:     "ozonpay",
			Amount:       rand.IntN(10000),
			PaymentDt:    28746374,
			Bank:         "ozon-bank",
			DeliveryCost: rand.IntN(2500),
			GoodsTotal:   rand.IntN(1000),
			CustomFee:    rand.IntN(1000),
		},
		Items: []models.Item{
			{
				ChrtID:      rand.IntN(100000),
				TrackNumber: "WBILMTESSTRACK",
				Price:       200000,
				Rid:         "gdhgdhgdohgdu454f",
				Name:        "macbook",
				Sale:        0,
				Size:        "15",
				TotalPrice:  200000,
				NmID:        rand.IntN(100000),
				Brand:       "Apple",
				Status:      200,
			},
		},
		Locale:            "ru",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "sdek",
		Shardkey:          "9",
		SmID:              rand.IntN(100),
		DateCreated:       time.Now().UTC(),
		OofShard:          "1",
	}
}

func sendMessage(w *kafka.Writer, order models.Order) {
	dataJSON, err := json.Marshal(order)
	if err != nil {
		log.Printf("failed to marshalling: %s", err)
		return
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(order.OrderUID),
			Value: []byte(dataJSON),
		})
		
	if err != nil {
		log.Printf("failed to write message: %s", err)
	} else {
		log.Printf("successfully write message")
	}
}
