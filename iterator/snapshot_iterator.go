package iterator

import (
	"context"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
	"encoding/binary"
)

type SnapshotIterator struct {
	client      *airtableclient.Client
	data        *airtableclient.Records
	internalPos int
}

func NewSnapshotIterator(ctx context.Context,client airtableclient.Client, config config.Config, pos sdk.Position) (*SnapshotIterator, error) {

	table := client.GetTable(config.BaseID, config.TableID)

	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		Do()
	if err != nil {
		//handle error records not returned
	}
	//records successful

	s := &SnapshotIterator{
		client:      client,
		data:        records,
		internalPos: ,
	}
	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	if s.internalPos == len(s.data.Records)+1 {
		return false
	}
	return true
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {

	if err := ctx.Err(); err != nil {
		return sdk.Record{}, err
	}

	s.internalPos++ // increment internal position
	rec := sdk.Util.Source.NewRecordSnapshot(
		s.buildRecordPosition(),
		s.buildRecordMetadata(),
		s.buildRecordKey(),
		s.buildRecordPayload(),
	)

	return rec, nil
}
func (s *SnapshotIterator) Stop() {
}

func (s *SnapshotIterator) buildRecordPosition() sdk.Position {
	position := s.data.Records[s.internalPos].ID //ID of individual record
	return sdk.Position(position)
}

func (s *SnapshotIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		"DatabaseID": config.BaseID,
		"TableID":    config.TableID,
	}
}

// buildRecordKey returns the key for the record.
func (s *SnapshotIterator) buildRecordKey(values []interface{}) sdk.Data {
	if s.keyColumnIndex == -1 {
		return nil
	}
	return sdk.StructuredData{
		// TODO handle composite keys
		s.config.KeyColumn: values[s.keyColumnIndex],
	}
}

func (s *SnapshotIterator) buildRecordPayload(values []interface{}) sdk.Data {
	payload := make(sdk.StructuredData)
	for i, val := range values {
		payload[s.config.Columns[i]] = val
	}
	return payload
}
