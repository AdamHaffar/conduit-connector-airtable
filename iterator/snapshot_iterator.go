package iterator

import (
	"context"
	"fmt"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	"github.com/AdamHaffar/conduit-connector-airtable/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
)

const lastmodified = "last-modified"

type SnapshotIterator struct {
	client   *airtableclient.Client
	data     *airtableclient.Records
	position position.Position
}

func NewSnapshotIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*SnapshotIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "NewSnapshotIterator").Logger()
	logger.Trace().Msg("Creating new snapshot iterator")

	table := client.GetTable(config.BaseID, config.TableID)
	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		Do()
	if err != nil {
		return &SnapshotIterator{}, fmt.Errorf("error while getting records")
	}

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return &SnapshotIterator{}, err
	}

	sdk.Logger(ctx).Info().Msgf("%v", records)
	s := &SnapshotIterator{
		client:   client,
		data:     records,
		position: NewPos,
	}

	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "HasNext").Logger()
	logger.Trace().Msg("HasNext()")

	if s.position.Offset == "" {
		return false
	}

	return true
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Next()")

	if err := ctx.Err(); err != nil {
		return sdk.Record{}, err
	}

	s.position.Index++ // increment internal position
	rec := sdk.Util.Source.NewRecordSnapshot(
		s.buildRecordPosition(),
		s.buildRecordMetadata(),
		s.buildRecordKey(),
		s.buildRecordPayload(),
	)

	return rec, nil

	//	get current record in page
	//	if record last in its page, get a new page. if no records
	//
	//
	//
}

func (s *SnapshotIterator) buildRecordPosition() sdk.Position {

	pos, err := s.position.ToRecordPosition()
	if err != nil {
		return nil
	}

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
	key := s.data.Records[s.position.Index-1].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *SnapshotIterator) buildRecordPayload() sdk.Data {
	payload := s.data.Records[s.position.Index-1].Fields
	return sdk.StructuredData{"Record Payload": payload}
}
