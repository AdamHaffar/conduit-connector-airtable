package config

import (
	"errors"
	"fmt"
	"strings"
)

const (
	APIKey  = "api_key"  //required
	BaseID  = "base_ID"  //required
	TableID = "table_ID" //required
)

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

type Config struct {
	APIKey  string
	BaseID  string
	TableID string
	URL     string
}

func ParseBaseConfig(cfg map[string]string) (Config, error) {

	err := checkEmpty(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("map must not be empty")
	}

	api, ok := cfg[APIKey]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", APIKey)
	}
	err = checkFormat(api, "key")
	if err != nil {
		return Config{}, err
	}

	base, ok := cfg[BaseID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", BaseID)
	}
	err = checkFormat(base, "app")
	if err != nil {
		return Config{}, err
	}

	table, ok := cfg[TableID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", TableID)
	}
	err = checkFormat(table, "tbl")
	if err != nil {
		return Config{}, err
	}

	return Config{
		APIKey:  cfg[APIKey],
		BaseID:  cfg[BaseID],
		TableID: cfg[TableID],
		URL:     "https://airtable.com/" + base + table,
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if cfg == nil || len(cfg) == 0 {
		return ErrEmptyConfig
	}
	return nil
}

func checkFormat(s string, ID string) error {

	if len(s) != 17 {
		return fmt.Errorf("id must be 17 characters long")
	}
	if !strings.HasPrefix(s, ID) {
		return fmt.Errorf("id must start with %v", ID)
	}
	return nil
}
