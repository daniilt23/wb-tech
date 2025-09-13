package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/daniilt23/wb-tech/app/consumer/database"

	"github.com/daniilt23/wb-tech/app/models"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

var (
	hash map[string]models.Order
	mu   sync.RWMutex
)

func main() {
	database.InitDB()

	initHash()

	go getMessage()

	startServer()
}

func initHash() {
	hash = make(map[string]models.Order)

	var orders []models.Order
	database.DB.Limit(3).Find(&orders)

	mu.Lock()
	defer mu.Unlock()
	for _, order := range orders {
		hash[order.OrderUID] = order
	}
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
	if err := database.DB.Omit("Delivery", "Payment", "Items").Create(&order).Error; err != nil {
		log.Printf("cannot add order to db %s", err)
		return
	}

	order.Delivery.OrderUID = order.OrderUID
	if err := database.DB.Create(&order.Delivery).Error; err != nil {
		log.Printf("cannot add delivery to db %s", err)
		return
	}

	order.Payment.OrderUID = order.OrderUID
	if err := database.DB.Create(&order.Payment).Error; err != nil {
		log.Printf("cannot add payment to db %s", err)
		return
	}

	for i := range order.Items {
		order.Items[i].OrderUID = order.OrderUID
		if err := database.DB.Create(&order.Items[i]).Error; err != nil {
			log.Printf("cannot add item to db %s", err)
			return
		}
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

	mu.RLock()
	order, isExist := hash[order_uid]
	mu.RUnlock()
	if isExist {
		c.IndentedJSON(http.StatusOK, order)
		log.Println("order from map")
		return
	}

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

	hash[order.OrderUID] = order
	log.Println("order from db")
	c.IndentedJSON(http.StatusOK, order)
}
