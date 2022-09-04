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

	sdk.Logger(ctx).Info().Msgf("%v %v", config.BaseID, config.TableID)

	table := client.GetTable(config.BaseID, config.TableID)
	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		Do()
	if err != nil {
		sdk.Logger(ctx).Info().Msgf("%v", err)
	}
	//records successful

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		sdk.Logger(ctx).Info().Msgf("##############BB")
	}
	//parse complete

	sdk.Logger(ctx).Info().Msgf("%v", records)
	s := &SnapshotIterator{
		client:      client,
		data:        records,
		internalPos: NewPos,
	}
	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	//sdk.Logger(ctx).Info().Msgf("index: %v || record: %v \n", s.internalPos.Index, s.data.Records[s.internalPos.Index].Fields)

	if s.internalPos.Index == len(s.data.Records) {
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
		s.buildRecordKey(ctx),
		s.buildRecordPayload(ctx),
	)
	//sdk.Logger(ctx).Info().Msgf("RETURNED: %v", s.data.Records[s.internalPos.Index-1].Fields)
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
func (s *SnapshotIterator) buildRecordKey(ctx context.Context) sdk.Data {
	key := s.data.Records[s.internalPos.Index-1].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *SnapshotIterator) buildRecordPayload(ctx context.Context) sdk.Data {
	payload := s.data.Records[s.internalPos.Index-1].Fields
	return sdk.StructuredData{"Record": payload}
}
