package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type Order struct {
	Name string `json:"name"`
}

func main() {
	http.HandleFunc("/order", placeOrder)
}

func PushOrderToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}
}

func placeOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid type of method", http.StatusMethodNotAllowed)
		return
	}
	
	order := new(Order)

	if err := json.NewDecoder(r.Body).Decode(order); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}