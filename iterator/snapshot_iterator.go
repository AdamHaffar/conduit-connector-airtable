package iterator

import (
	"context"
	"fmt"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	"github.com/AdamHaffar/conduit-connector-airtable/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
	"time"
)

type SnapshotIterator struct {
	client   *airtableclient.Client
	data     *airtableclient.Records
	position position.Position
	table    *airtableclient.Table
}

func NewSnapshotIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*SnapshotIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "NewSnapshotIterator").Logger()
	logger.Trace().Msg("Creating new snapshot iterator")

	table := client.GetTable(config.BaseID, config.TableID)

	records, err := table.GetRecords().
		InStringFormat("Europe/London", "en-gb").
		PageSize(20).
		Do()

	if err != nil {
		return &SnapshotIterator{}, fmt.Errorf("error while getting records")
	}

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return &SnapshotIterator{}, err
	}
	NewPos.Offset = records.Offset

	//sdk.Logger(ctx).Info().Msgf("%v", records)

	s := &SnapshotIterator{
		client:   client,
		data:     records,
		position: NewPos,
		table:    table,
	}

	return s, nil
}

func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "HasNext").Logger()
	logger.Trace().Msg("HasNext()")

	if s.position.Offset == "" { //Checks if last page (no offset)
		// at this point we switch to CDC
		return false
	}

	if s.position.RecordSlicePos >= len(s.data.Records)-1 { //Checks if last record in the page
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

	// get the record object, then transform it
	// currently, s.position.RecordSlicePos is incremented in buildRecordPosition
	// which means that you have to be careful and always call it first.
	// hence, we should change the buildXYZ methods to work on a record object
	// i.e. they shouldn't need to get the record themselves.
	s.position.RecordSlicePos++
	aRecord := s.data.Records[s.position.RecordSlicePos]
	pos, err := s.buildRecordPosition(aRecord)
	if err != nil {
		return sdk.Record{}, err
	}

	rec := sdk.Util.Source.NewRecordSnapshot(
		pos,
		s.buildRecordMetadata(aRecord),
		s.buildRecordKey(aRecord),
		s.buildRecordPayload(aRecord),
	)

	return rec, nil

	//	get current record in page
	//	if record last in its page, get a new page using offset.
	//  if offset = "" start again from offset 0
	//
	//
}

func (s *SnapshotIterator) GetPage(ctx context.Context) {
	logger := sdk.Logger(ctx).With().Str("Class", "snapshot_iterator").Str("Method", "NextPage").Logger()
	logger.Trace().Msg("NextPage()")

	r, err := s.table.GetRecords().
		// this probably won't work always,
		//depending on the time zone of the machine on which the connector is running
		InStringFormat("Europe/London", "en-gb").
		PageSize(20).
		WithOffset(s.position.Offset).
		WithSort(
			struct {
				FieldName string
				Direction string
			}{
				FieldName: "Name",
				Direction: "asc",
			}).
		Do()
	if err != nil {
		fmt.Printf("#error while getting records %v\n", err)
	}

	s.position.Offset = s.data.Offset
	s.data = r
	s.position.RecordSlicePos = 0
}

func (s *SnapshotIterator) buildRecordPosition() (sdk.Position, error) {

	s.position.RecordSlicePos++ // increment internal position
	s.position.Offset = s.data.Offset
	s.position.LastKnownTime = time.Now()

	pos, err := s.position.ToRecordPosition()
	if err != nil {
		return sdk.Position{}, fmt.Errorf("failed building Position: %w", err)
	}

	return pos, nil
}

func (s *SnapshotIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		"DatabaseID": config.BaseID,
		"TableID":    config.TableID,
	}
}

// buildRecordKey returns the key for the record.
func (s *SnapshotIterator) buildRecordKey() sdk.Data {
	key := s.data.Records[s.position.RecordSlicePos-1].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *SnapshotIterator) buildRecordPayload() sdk.Data {
	payload := s.data.Records[s.position.RecordSlicePos-1].Fields
	return sdk.StructuredData{"Record Payload": payload}
}
