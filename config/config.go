package config

import "errors"

const (
	GlobalConfigParam      = "global_config"
	SourceConfigParam      = "source_config"
	DestinationConfigParam = "destination_config"
)

var Required = []string{GlobalConfigParam}

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

type Config struct {
	globalConfigParam string
}

func ParseBaseConfig(cfg map[string]string) (Config, error) {
	err := checkEmpty(cfg)
	if err != nil {
		return Config{}, err
	}
	return Config{
		globalConfigParam: cfg[GlobalConfigParam],
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if cfg == nil || len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}
