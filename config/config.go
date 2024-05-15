package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"go.uber.org/zap"
)

type (
	Project struct {
		DataLink    `yaml:"datalink"`
		Transfer    `yaml:"transfer"`
		Application `yaml:"application"`
	}

	DataLink struct {
		Host string 	  `yaml:"host"`
		Port int 		  `yaml:"port"`
		RequestUrl string `yaml:"request_url"`
	}

	Transfer struct {
		Host string `yaml:"host"`
		Port int 	`yaml:"port"`
	}

	Application struct {
		Host string `yaml:"host"`
		Port int 	`yaml:"port"`
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