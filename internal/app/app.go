package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/services/kafka/producer"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/utils/functions"
	"github.com/satori/uuid"
	"go.uber.org/zap"
)

const (
	segmentLength = 100
	timestamptz = "2006-01-02 15:04:05+03:00"
) 


type TransferServer struct {
	producer sarama.SyncProducer
	logger  *zap.Logger
	cfg 	*config.Project
}

func NewTransferServer (producer sarama.SyncProducer, logger *zap.Logger, cfg *config.Project) *TransferServer {
	return &TransferServer{
		producer: producer,
		logger: logger,
		cfg: cfg,
	}
}

//DefineHandlers - method that defines handlers 
func DefineHadlers (mux *http.ServeMux, producer sarama.SyncProducer, consumer sarama.Consumer, logger *zap.Logger, cfg *config.Project) {
	//add custom valid tags for unmarshalling
	functions.InitDTOValidator()
	//initialization of the structure containing handlers
	srv := NewTransferServer(producer, logger, cfg)

	mux.HandleFunc("POST /api/v1/message", srv.message)
	mux.HandleFunc("POST /api/v1/segment", srv.segment)
}

//HANDLER
//message - method that receives dto.MessageFromAppLayer and splits message into segments (length is 100 bytes)
func (h *TransferServer) message(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
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
	isValid, err := bodyDTO.Validate()
	if err != nil || !isValid {
		h.logger.Error(err.Error()) 
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	go splitAndSendSegments(&bodyDTO, h.logger, h.cfg)
	w = functions.JsonResponse(w, map[string]string{"detail": "success"})
}

//splitAndSendSegments - function that splits message on segments and pushes them on datalink layer
func splitAndSendSegments(bodyDTO *dto.MessageFromAppLayer, logger *zap.Logger, cfg *config.Project) {
	rawMsg := []byte(bodyDTO.Message)
	contentLength := len(rawMsg)
	numberOfSegments := getNumberOfSegments(contentLength, segmentLength)
	var segmentId uint64 = 0 // segment_id
	for offset := 0; offset < contentLength; offset += segmentLength {
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
	defer func () {
		if resp != nil {
			err = resp.Body.Close()
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	} ()
	if err != nil {
		logger.Error(err.Error())
	}
}

func getNumberOfSegments(dataLength, segmentLength int) int {
	numberOfSegments := dataLength / segmentLength
	if dataLength % segmentLength != 0 {
		numberOfSegments++
	}
	return numberOfSegments
}

//HANDLER
//segment - method that receives dto.SegmentFromDatalinkLayer and push segments into Apache Kafka
func (h *TransferServer) segment(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		h.logger.Error(err.Error())
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	var bodyDTO dto.SegmentFromDatalinkLayer
	err = json.Unmarshal(body, &bodyDTO)
	if err != nil {
		h.logger.Error(err.Error())
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	isValid, err := bodyDTO.Validate()
	if err != nil || !isValid {
		h.logger.Error(err.Error())
		w = functions.ErrorResponse(w, err, http.StatusBadRequest)
		return
	}
	err = producer.SendKafkaMessage(h.producer, &bodyDTO, h.cfg)
	if err != nil {
		h.logger.Error(err.Error())
		w = functions.ErrorResponse(w, err, http.StatusInternalServerError)
		return
	}
	w = functions.JsonResponse(w, map[string]dto.SegmentFromDatalinkLayer{"detail": bodyDTO})
}