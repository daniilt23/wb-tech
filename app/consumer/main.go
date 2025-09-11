package main

import (
	"github.com/daniilt23/wb-tech/app/consumer/database"
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/daniilt23/wb-tech/app/models"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func main() {
	database.InitDB()

	//hashMap

	getMessage()

	startServer()
}

func getMessage() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())

		if err != nil {
			log.Printf("error reading message %s", err)
			continue
		}

		var order models.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Printf("error unmarshall data %s", err)
			continue
		}

		addRecord(order)
	}
}

func addRecord(order models.Order) {
	if err := database.DB.Create(&order).Error; err != nil {
		log.Printf("cannot add to db %s", err)
		return
	}

	log.Printf("successfully saved order: %s", order.OrderUID)
}

func startServer() {
	r := gin.Default()
	r.GET("order/:order_uid", getOrderById)

	r.Run(":8081")
}

func getOrderById(c *gin.Context) {
	order_uid := c.Param("order_uid")
	var order models.Order

	result := database.DB.Preload("Delivery").
		Preload("Payment").
		Preload("Items").
		First(&order, "order_uid = ?", order_uid)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		}
		return
	}

	c.IndentedJSON(http.StatusOK, order)
}
