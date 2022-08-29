package iterator

import (
	"context"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	"github.com/AdamHaffar/conduit-connector-airtable/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
)

type SnapshotIterator struct {
	client      *airtableclient.Client
	data        *airtableclient.Records
	internalPos position.Position
}

func NewSnapshotIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*SnapshotIterator, error) {

	table := client.GetTable(config.BaseID, config.TableID)
	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		Do()
	if err != nil {
		//handle error records not returned
	}
	//records successful

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		//parse error
	}
	//parse complete

	s := &SnapshotIterator{
		client:      client,
		data:        records,
		internalPos: NewPos,
	}
	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	if s.internalPos.Index == len(s.data.Records)+1 {
		return false
	}
	return true
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {

	if err := ctx.Err(); err != nil {
		return sdk.Record{}, err
	}

	s.internalPos.Index++ // increment internal position
	rec := sdk.Util.Source.NewRecordSnapshot(
		s.buildRecordPosition(),
		s.buildRecordMetadata(),
		s.buildRecordKey(),
		s.buildRecordPayload(),
	)

	return rec, nil
}

func (s *SnapshotIterator) buildRecordPosition() sdk.Position {

	pos, err := s.internalPos.ToRecordPosition()
	if err != nil {
		//marshall error
	}
	//marshall complete

	return pos
}

func (s *SnapshotIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		"DatabaseID": config.BaseID,
		"TableID":    config.TableID,
	}
}

// buildRecordKey returns the key for the record.
func (s *SnapshotIterator) buildRecordKey() sdk.Data {

	key := s.data.Records[s.internalPos.Index].ID //ID of individual record

	return sdk.StructuredData{
		"RecordID": key}
}

func (s *SnapshotIterator) buildRecordPayload() sdk.Data {
	payload := s.data.Records[s.internalPos.Index].Fields
	return sdk.StructuredData{"Record": payload}
}
