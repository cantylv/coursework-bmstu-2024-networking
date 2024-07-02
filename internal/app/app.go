package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/services/kafka/producer"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/utils/functions"
	"github.com/satori/uuid"
	"go.uber.org/zap"
)

// statuses of building message
const (
	InProgress = iota
	Collected
	Cancelled
)

type MessageBuilding struct {
	segments []dto.SegmentFromDatalinkLayer
	status   uint
}

type TransferServer struct {
	producer    sarama.SyncProducer
	logger      *zap.Logger
	cfg         *config.Project
	msgPipeline map[string]MessageBuilding
	mtx         *sync.RWMutex
}

func NewTransferServer(producer sarama.SyncProducer, logger *zap.Logger, cfg *config.Project) *TransferServer {
	return &TransferServer{
		producer:    producer,
		logger:      logger,
		cfg:         cfg,
		msgPipeline: map[string]MessageBuilding{},
		mtx:         &sync.RWMutex{},
	}
}

// DefineHandlers - method that defines handlers
func DefineHadlers(mux *http.ServeMux, cfg *config.Project) {
	logger := zap.Must(zap.NewDevelopment())
	//initialization kafka producer
	producer, err := producer.SetupProducer(cfg)
	if err != nil {
		logger.Fatal(err.Error())
	}
	//add custom valid tags for unmarshalling
	functions.InitDTOValidator()
	//initialization of the structure containing handlers
	srv := NewTransferServer(producer, logger, cfg)

	mux.HandleFunc("POST /api/v1/message", srv.Message)
	mux.HandleFunc("POST /api/v1/segment", srv.Segment)
	logger.Info("Successful definition of HTTP handlers")
}

// Message - receives dto.MessageFromAppLayer and splits message into segments and send them to Kafka (length is 100 bytes)
func (h *TransferServer) Message(w http.ResponseWriter, r *http.Request) {
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
	go h.splitAndSendSegments(&bodyDTO)
	w = functions.JsonResponse(w, map[string]string{"detail": "success"})
}

// splitAndSendSegments - splits message on segments and pushes them on datalink layer
func (h *TransferServer) splitAndSendSegments(bodyDTO *dto.MessageFromAppLayer) {
	rawMsg := []byte(bodyDTO.Message)
	contentLength := len(rawMsg)
	cfgTransfer := h.cfg.Transfer
	numberOfSegments := getNumberOfSegments(contentLength, cfgTransfer.SegmentLength)
	var segmentId uint64 = 0 // segment_id
	messageId := uuid.NewV4().String()
	for offset := 0; offset < contentLength; offset += cfgTransfer.SegmentLength {
		end := offset + cfgTransfer.SegmentLength
		if end > contentLength {
			end = contentLength
		}
		segment := dto.GetSegmentToDatalinkLayer(rawMsg[offset:end], numberOfSegments, segmentId, bodyDTO.Login, messageId)
		go h.sendKafkaMessage(segment)
		segmentId++
	}
}

// sendKafkaMessage - sends message into topic in Kafka
func (h *TransferServer) sendKafkaMessage(segment *dto.SegmentToDatalinkLayer) error {
	segmentJSON, err := json.Marshal(segment)
	if err != nil {
		return err
	}

	msg := sarama.ProducerMessage{
		Topic: h.cfg.Kafka.Topic,
		Value: sarama.StringEncoder(segmentJSON),
	}

	_, _, err = h.producer.SendMessage(&msg)
	if err != nil {
		return err
	}
	return nil
}

func getNumberOfSegments(dataLength, segmentLength int) int {
	numberOfSegments := dataLength / segmentLength
	if dataLength%segmentLength != 0 {
		numberOfSegments++
	}
	return numberOfSegments
}

// HANDLER
// segment - receives dto.SegmentFromDatalinkLayer and tries to collect message
func (h *TransferServer) Segment(w http.ResponseWriter, r *http.Request) {
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
	go h.tryToCollectMessage(bodyDTO)
	w = functions.JsonResponse(w, map[string]string{"detail": "success"})
}

// tryToCollectMessage - receives segments and collects message
func (h *TransferServer) tryToCollectMessage(segment dto.SegmentFromDatalinkLayer) {
	if segment.Error {
		return
	}
	msgId := segment.MessageId
	h.mtx.RLock()
	if messageState, ok := h.msgPipeline[msgId]; !ok {
		h.mtx.RUnlock()
		h.mtx.Lock()
		h.msgPipeline[msgId] = MessageBuilding{
			segments: append([]dto.SegmentFromDatalinkLayer{}, segment),
			status:   InProgress,
		}
		h.mtx.Unlock()
		go h.collectMessage(msgId)
	} else if messageState.status == InProgress {
		h.mtx.RUnlock()
		h.mtx.Lock()
		h.msgPipeline[msgId] = MessageBuilding{
			segments: append(h.msgPipeline[msgId].segments, segment),
			status:   h.msgPipeline[msgId].status,
		}
		h.mtx.Unlock()
		sortNumbers(h.msgPipeline[msgId].segments)
	}
}

// bubble sort
func sortNumbers(segments []dto.SegmentFromDatalinkLayer) {
	n := len(segments)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if segments[j].SegmentId > segments[j+1].SegmentId {
				segments[j], segments[j+1] = segments[j+1], segments[j]
			}
		}
	}
}

// builds message max number of tries is h.cfg.Transfer.MaxNumberOfAttempts (default = 3)
func (h *TransferServer) collectMessage(msgId string) {
	for i := 0; i < h.cfg.Transfer.MaxNumberOfAttempts && h.msgPipeline[msgId].status == InProgress; i++ {
		ticker := make(chan bool)
		go func(tickerChan chan bool) {
			time.Sleep(h.cfg.Transfer.Timeout)
			ticker <- true
			close(ticker)
		}(ticker)
		var wg sync.WaitGroup
		wg.Add(1)
		go func(tickerChannel chan bool) {
			defer wg.Done()
			for {
				select {
				case <-tickerChannel:
					{
						return
					}
				default:
					{
						h.mtx.RLock()
						msgSegments := h.msgPipeline[msgId].segments
						h.mtx.RUnlock()
						msgSegmentsCount := msgSegments[0].NumberOfSegments
						if msgSegmentsCount != uint64(len(msgSegments)) {
							continue
						}
						var rawMessage []byte
						for j := 0; j < int(msgSegmentsCount); j++ {
							rawMessage = append(rawMessage, msgSegments[j].Data...)
						}
						stringMessage := string(rawMessage)
						msg := &dto.MessageToAppLayer{
							Message: stringMessage,
							Login:   h.msgPipeline[msgId].segments[0].Login,
							Date:    h.msgPipeline[msgId].segments[0].DateSend,
							Error:   false,
						}
						go h.sendMessage(msg)
						h.mtx.Lock()
						h.msgPipeline[msgId] = MessageBuilding{
							segments: msgSegments,
							status:   Collected,
						}
						h.mtx.Unlock()
						return
					}
				}
			}
		}(ticker)
		wg.Wait()
	}
	if h.msgPipeline[msgId].status == InProgress {
		h.msgPipeline[msgId] = MessageBuilding{
			segments: h.msgPipeline[msgId].segments,
			status:   Cancelled,
		}
	}
}

// sendSegment - sends segment to Datalink Layer
func (h *TransferServer) sendMessage(msg *dto.MessageToAppLayer) {
	cfgApplication := h.cfg.Application
	url := fmt.Sprintf("http://%s:%d/%s", cfgApplication.Host, cfgApplication.Port, cfgApplication.RequestUrl)
	reqBody, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error(err.Error())
	}
	bodyReader := bytes.NewBuffer(reqBody)
	resp, err := http.Post(url, "application/json", bodyReader)
	defer func() {
		if resp != nil {
			err = resp.Body.Close()
			if err != nil {
				h.logger.Error(err.Error())
			}
		}
	}()
	if err != nil {
		h.logger.Error(err.Error())
	}
}
