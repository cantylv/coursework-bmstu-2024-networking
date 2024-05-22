package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
	"go.uber.org/zap"
)

type DemonFunction struct {
    consumer sarama.Consumer
    logger *zap.Logger
    cfg *config.Project
}

func InitDemon(cfgProject *config.Project) {
    logs := zap.Must(zap.NewProduction())
    cons, err := setupConsumer(cfgProject)
    if err != nil {
        logs.Fatal(err.Error())
    }
    demonEngine := DemonFunction{
        consumer: cons,
        logger: logs,
        cfg: cfgProject,
    }
    var wg sync.WaitGroup
    wg.Add(1)
    go demonEngine.SendSegmentToDatalinkLayer(&wg)
    wg.Wait()
}

func setupConsumer(cfg *config.Project) (sarama.Consumer, error){
    config := sarama.NewConfig()
	kafkaAddress := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
    consumer, err := sarama.NewConsumer([]string{kafkaAddress}, config)
    if err != nil {
        return nil, fmt.Errorf("SetupConsumer error: %v", err)
    }
    return consumer, nil
}

//SendSegmentToDatalinkLayer - starts goroutine that takes segments from Kafka and sends them to Datalink Layer
func (d *DemonFunction) SendSegmentToDatalinkLayer(wgParent *sync.WaitGroup) {
    //channel for communication between goroutines
    segmentChannel := make(chan dto.SegmentToDatalinkLayer)

    partitions, err := d.consumer.Partitions(d.cfg.Kafka.Topic)
    if err != nil {
        d.logger.Fatal(err.Error())
        return
    }
    var wg sync.WaitGroup
    //channels to stop goroutines
    doneChannels := make([]chan bool, len(partitions))
    for index, partition := range partitions {
        partitionConsumer, err := d.consumer.ConsumePartition(d.cfg.Kafka.Topic, partition, sarama.OffsetNewest)
        if err != nil {
            d.logger.Error(err.Error())
            continue
        }
        doneChannels[index] = make(chan bool)
        wg.Add(1)
        go d.getSegmentOfMessage(partitionConsumer, segmentChannel, doneChannels[index], &wg)
        defer func(chanDone chan bool, chanIndex int) {
            chanDone <- true
        }(doneChannels[index], index)
    }
    for segment := range segmentChannel {
        go d.sendSegment(segment)
    } 
    wgParent.Done()
}

//getSegmentOfMessage - writes segment from Kafka into channel
func (d *DemonFunction) getSegmentOfMessage (partitionConsumer sarama.PartitionConsumer, segmentChannel chan dto.SegmentToDatalinkLayer, done chan bool, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        select {
            case data, ok := <-partitionConsumer.Messages(): {
                if !ok {
                    return
                }
                var segment dto.SegmentToDatalinkLayer 
                err := json.Unmarshal(data.Value, &segment)
                if err != nil {
                    d.logger.Error(err.Error())
                    continue
                }
                segmentChannel <- segment
            }
            case <-done: {
                err := partitionConsumer.Close() 
                if err != nil {
                    d.logger.Error(err.Error())
                }
                return
            }
        }
    }
}

//sendSegment - sends segment to Datalink Layer
func (d *DemonFunction) sendSegment(segment dto.SegmentToDatalinkLayer) {
    cfgDatalink := d.cfg.DataLink
	url := fmt.Sprintf("http://%s:%d/%s", cfgDatalink.Host, cfgDatalink.Port, cfgDatalink.RequestUrl)
	reqBody, err := json.Marshal(segment)
	if err != nil {
		d.logger.Error(err.Error())
	}
	bodyReader := bytes.NewBuffer(reqBody)
	resp, err := http.Post(url, "application/json", bodyReader)
	defer func () {
		if resp != nil {
			err = resp.Body.Close()
			if err != nil {
				d.logger.Error(err.Error())
			}
		}
	} ()
	if err != nil {
		d.logger.Error(err.Error())
	}
}