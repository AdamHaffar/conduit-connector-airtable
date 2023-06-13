package airtable

import (
	"context"
	"github.com/conduitio-labs/conduit-connector-airtable/config"
	"github.com/matryer/is"
	"testing"
)

const (
	testApiKey    = "key01234567890123"
	testBaseID    = "app01234567890123"
	testTableID   = "tbl01234567890123"
	testEnableCDC = "f"
)

func TestSource_Configure(t *testing.T) {
	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.APIKey:    testApiKey,
		config.BaseID:    testBaseID,
		config.TableID:   testTableID,
		config.EnableCDC: testEnableCDC,
	})
	is.NoErr(err)
	is.Equal(s.config, config.Config{
		APIKey:    "key01234567890123",
		BaseID:    "app01234567890123",
		TableID:   "tbl01234567890123",
		EnableCDC: false,
	})
}
