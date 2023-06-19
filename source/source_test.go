package airtable

import (
	"context"
	"errors"
	"github.com/conduitio-labs/conduit-connector-airtable/config"
	"github.com/conduitio-labs/conduit-connector-airtable/source/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	"testing"
)

const (
	testApiKey    = "key01234567890123"
	testBaseID    = "app01234567890123"
	testTableID   = "tbl01234567890123"
	testEnableCDC = "f"
)

func TestSource_Configure_success(t *testing.T) {
	t.Parallel()

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

func TestSource_Configure_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.APIKey:    testApiKey,
		config.BaseID:    testBaseID,
		config.EnableCDC: testEnableCDC,
	})
	is.Equal(err.Error(), `couldn't parse the source config: "table_ID" config value must be set`)
}

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(sdk.StructuredData)
	st["key"] = "value"

	record := sdk.Record{
		Position: sdk.Position(`{"last_processed_element_value": 1}`),
		Metadata: nil,
		Key:      st,
		Payload:  sdk.Change{After: st},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		iterator: it,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)

	is.Equal(r, record)
}

func TestSource_Read_failureHasNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(false)

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "backoff retry")
}

func TestSource_Read_failureNext(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true)
	it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "couldn't fetch the records: key is not exist")
}

func TestSource_Open_failureSnapshot(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true)
	it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.Equal(err.Error(), "couldn't fetch the records: key is not exist")
}
