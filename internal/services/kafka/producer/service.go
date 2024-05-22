package producer

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/cantylv/coursework-bmstu-2024-networking/config"
	"github.com/cantylv/coursework-bmstu-2024-networking/internal/entity/dto"
)

//SetupProduces - function that creates new produces for Apache Kafka Cluster
func SetupProducer(cfg *config.Project) (sarama.SyncProducer, error) {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true

    producer, err := sarama.NewSyncProducer([]string{fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)}, config)
    if err != nil {
		return nil, err
    }
    return producer, nil
}