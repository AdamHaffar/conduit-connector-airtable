package iterator

import (
	"context"
	"github.com/conduitio-labs/conduit-connector-airtable/config"
	mock_iterator "github.com/conduitio-labs/conduit-connector-airtable/iterator/mock"
	"github.com/conduitio-labs/conduit-connector-airtable/position"
	"github.com/golang/mock/gomock"
	is "github.com/matryer/is"
	airtableclient "github.com/mehanizm/airtable"
	"testing"
	"time"
)

const (
	testApiKey    = "key01234567890123"
	testBaseID    = "app01234567890123"
	testTableID   = "tbl01234567890123"
	testEnableCDC = "f"
)

func Test_NewSnapshotIterator(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	ctx := context.Background()
	ctrl := gomock.NewController(t)

	MockClient := mock_iterator.NewMockAirtableClientInterface(ctrl)

	mockTable := airtableclient.Table{}
	configVAR := config.Config{BaseID: "base1", TableID: "table1"}
	pos := position.Position{
		RecordSlicePos:   0,
		Offset:           "",
		LastKnownTime:    time.Time{},
		LastKnownRecord:  "",
		SnapshotFinished: false,
	}

	sdkPos, err := pos.ToRecordPosition()

	MockClient.EXPECT().GetTable("base1", "table1").Return(&mockTable)

	snapshotIterator, err := NewSnapshotIterator(ctx, MockClient, configVAR, sdkPos)

	is.Equal(snapshotIterator, &SnapshotIterator{
		currentPageRecords: nil,
		position:           pos,
		table:              &mockTable,
		config:             config.Config{BaseID: "base1", TableID: "table1"},
		client:             MockClient,
	})

	is.NoErr(err)
}
