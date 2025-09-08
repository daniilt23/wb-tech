package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	r.GET("get", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, gin.H{"SMS": 6})
	})

	r.Run()
}