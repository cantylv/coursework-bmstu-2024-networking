package main

import (
	"net/http"
	"os"

	"github.com/cantylv/coursework-bmstu-2024-networking/internal/app"
	"go.uber.org/zap"
)

func main() {
	// initialization logger 
	logger := zap.Must(zap.NewDevelopment()) 
	// initialization multiplexor
	mux := http.NewServeMux()
	// definition of handlers
	app.DefineHadlers(mux, logger)
	// listening on port = $SERVERPORT
	logger.Fatal(http.ListenAndServe(":" + os.Getenv("SERVERPORT"), mux).Error())
}