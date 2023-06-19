package iterator

import (
	mock "github.com/conduitio-labs/conduit-connector-airtable/iterator/mock"
	"github.com/golang/mock/gomock"
	"testing"
)

const (
	testApiKey    = "key01234567890123"
	testBaseID    = "app01234567890123"
	testTableID   = "tbl01234567890123"
	testEnableCDC = "f"
)

func TestNewSnapshotIterator_failure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	it := mock.NewMockAirtableClientInterface(ctrl)
	it.EXPECT().GetTable(testBaseID, testTableID).Return(nil)

	//s := SnapshotIterator{
	//	config:
	//}
	//
	//_, err := s.Read(ctx)
	//is.Equal(err.Error(), "couldn't fetch the records: key is not exist")
}
