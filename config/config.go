package config

import (
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"go.uber.org/zap"
)

type (
	Project struct {
		DataLink    `yaml:"datalink"`
		Transfer    `yaml:"transfer"`
		Application `yaml:"application"`
		Kafka 		`yaml:"kafka"`
	}

	DataLink struct {
		Host string 	  `yaml:"host"`
		Port int 		  `yaml:"port"`
		RequestUrl string `yaml:"request_url"`
	}

	Transfer struct {
		Host string 		    `yaml:"host"`
		Port int 			    `yaml:"port"`
		Timeout time.Duration   `yaml:"timeout"`
		SegmentLength int 	    `yaml:"segment_length"`
		MaxNumberOfAttempts int `yaml:"max_number_of_attempts"`
	}

	Application struct {
		Host string `yaml:"host"`
		Port int 	`yaml:"port"`
		RequestUrl string `yaml:"request_url"`
	}

	Kafka struct {
		Host  		  string `yaml:"host"`
		Port  		  int 	 `yaml:"port"`
		Topic 		  string `yaml:"topic"`
		ConsumerGroup string `yaml:"consumer-group"`
	}
)

func NewConfig(logger *zap.Logger) *Project {
	cfg := &Project{}

	err := cleanenv.ReadConfig("config/config.yaml", cfg)
	if err != nil {
		logger.Fatal(err.Error())
	}

	err = cleanenv.ReadEnv(cfg)
	if err != nil {
		logger.Fatal(err.Error())
	}

	logger.Info("Reading configuration successful")
	return cfg
}