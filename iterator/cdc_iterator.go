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

const lastmodified = "last-modified"

type CDCIterator struct {
	client   *airtableclient.Client
	data     *airtableclient.Records
	position position.Position
	table    *airtableclient.Table
	config   config.Config
}

func NewCDCIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*CDCIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "NewCDCIterator").Logger()
	logger.Trace().Msg("Creating new cdc iterator")

	table := client.GetTable(config.BaseID, config.TableID)
	IteratorConfig := config

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return &CDCIterator{}, err
	}

	//if pos == nil { //if no previous position, start from time 0

	//logger.Warn().Msgf("POS=nil\n")

	NewPos.Offset = "0"
	NewPos.LastKnownTime = time.Date(0001, 1, 1, 00, 00, 00, 00, time.UTC)
	NewPos.RecordSlicePos = 0
	NewPos.LastKnownRecord = ""
	//}

	s := &CDCIterator{
		client:   client,
		data:     nil,
		position: NewPos,
		table:    table,
		config:   IteratorConfig,
	}

	s.GetRecords(ctx)
	logger.Trace().Msgf("%v\n", s.position.LastKnownTime)
	return s, nil
}

func (s *CDCIterator) GetRecords(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "GetRecords").Logger()
	logger.Trace().Msgf("Position: %v\n", s.position)
	logger.Trace().Msgf("Position: %v\n", s.position.Offset)

	timeString := s.position.LastKnownTime.Format("2/1/2006 15:04:05")
	queryString := "LAST_MODIFIED_TIME()>DATETIME_PARSE(\"" + timeString + "\", 'D/M/YYYY HH:mm:ss')"

	records, err := s.table.GetRecords().InStringFormat("Europe/London", "en-gb").
		PageSize(5).
		WithFilterFormula(queryString).
		WithSort(struct {
			FieldName string
			Direction string
		}{FieldName: lastmodified, Direction: "asc"}).
		WithOffset(s.position.Offset).
		Do()
	if err != nil {
		return fmt.Errorf("error while getting records")
	}

	s.data = records
	logger.Warn().Msgf("Data: %v\n", s.data.Records)
	logger.Warn().Msgf("querystring: %v\n", queryString)
	logger.Warn().Msgf("current time: %v\n", time.Now())
	return nil
}

func (s *CDCIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "HasNext").Logger()
	logger.Trace().Msg("HasNext()")

	//logger.Warn().Msgf("%v\n", s.position.RecordSlicePos)
	//logger.Warn().Msgf("%v\n", len(s.data.Records))
	//logger.Warn().Msgf("%v\n", s.data.Offset)

	if s.position.RecordSlicePos >= len(s.data.Records) { //if end of page has been reached
		s.position.RecordSlicePos = 0

		if s.data.Offset == "" { //if there are not more pages left
			s.GetRecords(ctx)
			return false
		}
		s.GetRecords(ctx)
	}

	//if s.position.LastKnownTime == time.Now().Round(time.Minute) {
	//	return false
	//}

	return true
}

func (s *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Next()")
	if err := ctx.Err(); err != nil {
		return sdk.Record{}, err
	}

	s.position.RecordSlicePos++
	pos, err := s.buildRecordPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	rec := sdk.Util.Source.NewRecordSnapshot(
		pos,
		s.buildRecordMetadata(),
		s.buildRecordKey(),
		s.buildRecordPayload(),
	)

	//CreatedTime, err := time.Parse("2/1/2006 15:04:05", s.data.Records[s.position.RecordSlicePos].CreatedTime)

	//if CreatedTime > s.position.LastKnownTime

	return rec, nil
}

func (s *CDCIterator) buildRecordPosition() (sdk.Position, error) {

	s.position.Offset = s.data.Offset
	timePos, err := time.Parse("2/1/2006 15:04:05", s.data.Records[s.position.RecordSlicePos-1].Fields["datetime-str"].(string))
	if err != nil {
		return sdk.Position{}, err
	}
	s.position.LastKnownTime = timePos

	pos, err := s.position.ToRecordPosition()
	if err != nil {
		return sdk.Position{}, fmt.Errorf("failed building Position: %w", err)
	}

	return pos, nil
}

func (s *CDCIterator) buildRecordMetadata() map[string]string {
	return map[string]string{
		"DatabaseID": s.config.BaseID,
		"TableID":    s.config.TableID,
	}
}

func (s *CDCIterator) buildRecordKey() sdk.Data {
	key := s.data.Records[s.position.RecordSlicePos-1].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *CDCIterator) buildRecordPayload() sdk.Data {
	payload := s.data.Records[s.position.RecordSlicePos-1].Fields
	return sdk.StructuredData{"Record Payload": payload}
}
