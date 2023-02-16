package iterator

import (
	"context"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-airtable/config"
	"github.com/conduitio-labs/conduit-connector-airtable/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
	"time"
)

type SnapshotIterator struct {
	client             *airtableclient.Client
	currentPageRecords *airtableclient.Records
	position           position.Position
	table              *airtableclient.Table
	config             config.Config
}

func NewSnapshotIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*SnapshotIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "NewSnapshotIterator").Logger()
	logger.Trace().Msg("Creating new snapshot iterator")

	table := client.GetTable(config.BaseID, config.TableID)

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("error while parsing record position: %w", err)
	}

	s := &SnapshotIterator{
		client:             client,
		currentPageRecords: nil,
		position:           NewPos,
		table:              table,
		config:             config,
	}

	logger.Trace().Msgf("currentPageRecords: %v ", s.currentPageRecords.Records)

	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "HasNext").Logger()
	logger.Trace().Msgf("offset: %v ", s.position.Offset)

	if s.position.RecordSlicePos >= len(s.currentPageRecords.Records)-1 { //Checks if last record in the page

		if s.position.Offset == "" { //Checks if last page (no offset)
			return false
		}

		s.GetPage(ctx)
	}

	return true
}

func (s *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Next()")
	if err := ctx.Err(); err != nil {
		return sdk.Record{}, err
	}

	// increment internal position
	s.position.RecordSlicePos++

	pos, err := s.buildRecordPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed buildiong position: %w", err)
	}

	rec := sdk.Util.Source.NewRecordSnapshot(
		pos,
		s.buildRecordMetadata(),
		s.buildRecordKey(),
		s.buildRecordPayload(),
	)

	return rec, nil
}

func (s *SnapshotIterator) GetPage(ctx context.Context) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "NextPage").Logger()
	logger.Trace().Msg("NextPage()")

	r, err := s.table.GetRecords().
		PageSize(20).
		WithOffset(s.position.Offset).
		WithSort(struct {
			FieldName string
			Direction string
		}{FieldName: "last-modified", Direction: "asc"}).
		Do()
	if err != nil {
		fmt.Printf("#error while getting records %v\n", err)
	}

	s.position.Offset = s.currentPageRecords.Offset
	s.currentPageRecords = r
	s.position.RecordSlicePos = 0
}

func (s *SnapshotIterator) buildRecordPosition() (sdk.Position, error) {

	s.position.Offset = s.currentPageRecords.Offset
	s.position.LastKnownTime = time.Now()

	pos, err := s.position.ToRecordPosition()
	if err != nil {
		return sdk.Position{}, fmt.Errorf("failed marshalling Position: %w", err)
	}

	return pos, nil
}

func (s *SnapshotIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		"DatabaseID": s.config.BaseID,
		"TableID":    s.config.TableID,
	}
}

// buildRecordKey returns the key for the record.
func (s *SnapshotIterator) buildRecordKey() sdk.Data {
	key := s.currentPageRecords.Records[s.position.RecordSlicePos-1].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *SnapshotIterator) buildRecordPayload() sdk.Data {
	payload := s.currentPageRecords.Records[s.position.RecordSlicePos-1].Fields
	return sdk.StructuredData{"Record Payload": payload}
}
