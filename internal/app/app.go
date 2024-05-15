package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/utils/functions"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	segmentLength = 100
	timestamptz = "2006-01-02 15:04:05-07:00"
) 


type TransferServer struct {
	logger  *zap.Logger
	cfg 	*config.Project
}

func NewTransferServer (logger *zap.Logger, cfg *config.Project) *TransferServer {
	return &TransferServer{
		logger: logger,
		cfg: cfg,
	}
}

//DefineHandlers - method that defines handlers 
func DefineHadlers (mux *http.ServeMux, logger *zap.Logger) {
	//add custom valid tags for unmarshalling
	functions.InitDTOValidator()
	//reading config 
	cfgProject := config.NewConfig(logger)
	//initialization of the structure containing handlers
	srv := NewTransferServer(logger, cfgProject)

	mux.HandleFunc("POST /api/v1/message", srv.message)
	mux.HandleFunc("POST /api/v1/segment", srv.segment)
	mux.HandleFunc("GET /api/v1/send", srv.send)
}

//message - method that receives dto.MessageFromAppLayer and splits message into segments (length is 100 bytes)
func (h *TransferServer) message(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error(err.Error())
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	var bodyDTO dto.MessageFromAppLayer
	err = json.Unmarshal(body, &bodyDTO) 
	if err != nil {
		h.logger.Error(err.Error()) 
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	splitAndSendSegments(&bodyDTO, h.logger, h.cfg)
	w = functions.JsonResponse(w, map[string]string{"detail": "success"})
}

//splitAndSendSegments - function that splits message on segments and pushes them on datalink layer
func splitAndSendSegments(bodyDTO *dto.MessageFromAppLayer, logger *zap.Logger, cfg *config.Project) {
	rawMsg := []byte(bodyDTO.Message)
	contentLength := len(rawMsg)
	numberOfSegments := getNumberOfSegments(len(rawMsg), segmentLength)
	var segmentId uint64 = 0 // segment_id
	for offset := 0; offset < len(rawMsg); offset += segmentLength {
		end := offset + segmentLength
		if end > contentLength {
			end = contentLength
		}
		go sendSegment(rawMsg[offset:end], numberOfSegments, segmentId,  bodyDTO.Login, logger, cfg) 
		segmentId++
	}
}

func sendSegment(rawData []byte, numberOfSegments int, segmentId uint64, login string, logger *zap.Logger, cfg *config.Project) {
	url := fmt.Sprintf("http://%s:%d/%s", cfg.DataLink.Host, cfg.DataLink.Port, cfg.DataLink.RequestUrl)
	reqData := dto.SegmentToDatalinkLayer {
		Data: rawData,
		SegmentId: segmentId,
		NumberOfSegments: numberOfSegments,
		DateSend: time.Now().UTC().Format(timestamptz),
		Login: login,
		MessageId: uuid.NewV4().String(),
	}
	reqBody, err := json.Marshal(reqData)
	if err != nil {
		logger.Error(err.Error())
	}
	bodyReader := bytes.NewBuffer(reqBody)
	resp, err := http.Post(url, "application/json", bodyReader)
	if err != nil {
		logger.Error(err.Error())
	}
	defer resp.Body.Close()
	logger.Info(fmt.Sprintf("%v", resp))
}

func getNumberOfSegments(dataLength, segmentLength int) int {
	numberOfSegments := dataLength / segmentLength
	if dataLength % segmentLength != 0 {
		numberOfSegments++
	}
	return numberOfSegments
}

//segment - method that receives dto.SegmentFromDatalinkLayer and push segments into Apache Kafka
func (h *TransferServer) segment(w http.ResponseWriter, r *http.Request) {
	w = functions.JsonResponse(w, map[string]string{"detail": "You need to implement TransferServer.segment"})
}

//send - method that requests segments from Apache kafka and tries to collect message and sends to application layer 
func (h *TransferServer) send(w http.ResponseWriter, r *http.Request) {
	w = functions.JsonResponse(w, map[string]string{"detail": "You need to implement TransferServer.send"})
}