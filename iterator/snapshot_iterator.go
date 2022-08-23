package iterator

import (
	"context"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
)

type SnapshotIterator struct {
	client   *airtableclient.Client
	data     *airtableclient.Records
	position int
}

func NewSnapshotIterator(client airtableclient.Client, config config.Config, pos int) (*SnapshotIterator, error) {

	table := client.GetTable(config.BaseID, config.TableID)

	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		Do()
	if err != nil {
		//handle error records not returned
	}
	//records successful

	s := &SnapshotIterator{
		client:   &client,
		data:     records,
		position: pos,
	}
	return s, nil
}

func (w *SnapshotIterator) HasNext(ctx context.Context) bool {
	if w.position == len(w.data.Records) {
		return false
	}
	return true
}

func (w *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	return nil
}
func (w *SnapshotIterator) Stop() {
}

func (s *SnapshotIterator) prepareRecord(ctx context.Context, data []string) (sdk.Record, error) {
}
