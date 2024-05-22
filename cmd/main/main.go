package main

import (
	"net/http"
	"strconv"

	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/app"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/services/kafka/consumer"
	"go.uber.org/zap"
)

// Дополнительное задание:
// Транспортный - переделать использование Kafka, она должна применяться не при сборке сегментов,
// а при сегментации (соответствовать Congestion Control TCP)

func main() {
	//initialization logger 
	logger := zap.Must(zap.NewDevelopment()) 
	//initialization multiplexor
	mux := http.NewServeMux()
	//reading config 
	cfgProject := config.NewConfig(logger)
	//definition of handlers
	app.DefineHadlers(mux, cfgProject)
	//initializes goroutines which work with Kafka 
	go consumer.InitDemon(cfgProject)
	logger.Fatal(http.ListenAndServe(":" + strconv.Itoa(cfgProject.Transfer.Port), mux).Error())
}