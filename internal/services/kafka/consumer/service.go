package consumer

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
	"go.uber.org/zap"
)

func CollectMessage(consumer sarama.Consumer, topic string, logger *zap.Logger) (*dto.MessageToAppLayer, error) {
    //get IDs of topic's partitions
    defer fmt.Println("collecting message was stopped")
    for {
        partitions, err := consumer.Partitions(topic)
        if err != nil {
            return nil, err
        }
        segmentBuffer := []dto.SegmentFromDatalinkLayer{}
        for _, partition := range partitions {
            partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
            if err != nil {
                logger.Error(err.Error())
                continue
            }
            defer func () {
                err = partitionConsumer.Close() 
                if err != nil {
                    logger.Error(err.Error())
                }
            } () 
            
            for data := range partitionConsumer.Messages() {
                var segment dto.SegmentFromDatalinkLayer 
                err := json.Unmarshal(data.Value, &segment)
                if err != nil {
                    logger.Error(err.Error())
                    continue
                }
                segmentBuffer = append(segmentBuffer, segment)
            }
        }
        
    }
}

func SetupConsumer(cfg *config.Project) (sarama.Consumer, error){
    config := sarama.NewConfig()

	kafkaAddress := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
    consumer, err := sarama.NewConsumer([]string{kafkaAddress}, config)
    if err != nil {
        return nil, fmt.Errorf("SetupConsumer error: %v", err)
    }
    return consumer, nil
}



