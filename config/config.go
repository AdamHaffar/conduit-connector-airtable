package config

import (
	"errors"
	"fmt"
	"strings"
)

const (
	BaseID   = "base_ID" //required
	TableID  = "table_ID"
	ViewID   = "view_ID"
	RecordID = "record_ID"
)

var (
	ErrEmptyConfig = errors.New("missing or empty config")
)

type Config struct {
	baseID   string
	tableID  string
	viewID   string
	recordID string
}

func ParseBaseConfig(cfg map[string]string) (Config, error) {

	err := checkEmpty(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("map must not be empty")
	}

	BaseID, ok := cfg[BaseID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", BaseID)
	}
	err = checkFormat(BaseID, "app")
	if err != nil {
		return Config{}, err
	}

	TableID, ok := cfg[TableID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", TableID)
	}
	err = checkFormat(TableID, "tbl")
	if err != nil {
		return Config{}, err
	}

	ViewID, ok := cfg[ViewID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", ViewID)
	}
	err = checkFormat(ViewID, "viw")
	if err != nil {
		return Config{}, err
	}

	RecordID, ok := cfg[RecordID]
	if !ok {
		return Config{}, fmt.Errorf("%q config value must be set", RecordID)
	}
	err = checkFormat(RecordID, "rec")
	if err != nil {
		return Config{}, err
	}

	return Config{
		baseID:   cfg[BaseID],
		tableID:  cfg[TableID],
		viewID:   cfg[ViewID],
		recordID: cfg[RecordID],
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
