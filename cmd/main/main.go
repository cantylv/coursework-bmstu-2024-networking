package main

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/app"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/services/kafka/consumer"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/services/kafka/producer"
	"go.uber.org/zap"
)

func main() {
	//initialization logger 
	logger := zap.Must(zap.NewDevelopment()) 
	//initialization multiplexor
	mux := http.NewServeMux()
	//reading config 
	cfgProject := config.NewConfig(logger)

	//initialization kafka producer 
	prod, err := producer.SetupProducer(cfgProject)
	defer func() {
		err = prod.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	} () 
	if err != nil {
		logger.Fatal(err.Error())
	}
	//initialization kafka consumer
	cons, err := consumer.SetupConsumer(cfgProject)
	defer func() {
		err = cons.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	} () 
	if err != nil {
		logger.Fatal(err.Error())
	}
	//definition of handlers
	app.DefineHadlers(mux, prod, cons, logger, cfgProject)

	//start goroutine that recover message from segments
	go consumer.CollectMessage(cons, cfgProject.Kafka.Topic, logger)
	logger.Fatal(http.ListenAndServe(":" + strconv.Itoa(cfgProject.Transfer.Port), mux).Error())
}