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

const LASTMODIFIED = "last-modified"
const LASTMODIFIEDSTR = "last-modified-str"

type CDCIterator struct {
	client             *airtableclient.Client
	currentPageRecords *airtableclient.Records
	position           position.Position
	table              *airtableclient.Table
	config             config.Config
}

func NewCDCIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*CDCIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "NewCDCIterator").Logger()
	logger.Trace().Msg("Creating new cdc iterator")

	table := client.GetTable(config.BaseID, config.TableID) //Airtable client 	//

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return &CDCIterator{}, fmt.Errorf("error while parsing record position: %w", err)
	}

	//if no previous position exists, set a new one
	if pos == nil {
		NewPos.Offset = "0"
		NewPos.LastKnownTime = time.Date(0001, 1, 1, 00, 00, 00, 00, time.UTC)
		NewPos.LastKnownRecord = ""
		NewPos.SnapshotFinished = false
	}
	NewPos.RecordSlicePos = -1 //always resets the internal position

	s := &CDCIterator{
		client:             client,
		currentPageRecords: nil,
		position:           NewPos,
		table:              table,
		config:             config,
	}
	err = s.GetRecords(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *CDCIterator) GetRecords(ctx context.Context) error {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "GetRecords").Logger()
	logger.Trace().Msg("Getting Airtable Records")

	//inserts the LastKnownTime from sdk.Position into the querystring
	timeString := s.position.LastKnownTime.Format("2/1/2006 15:04:05")
	queryString := "LAST_MODIFIED_TIME()>DATETIME_PARSE(\"" + timeString + "\", 'D/M/YYYY HH:mm:ss')"

	/*
		Airtable client GET request

		-Uses queryString to get all records that have been modified AFTER LastKnownTime in sdk.Position
		-5 Records per request (adjustable)
		-SORTS by last modified time to get handle records in order of oldest -> latest.
		-Offset is passed to navigate index different pages (as only 5 records per request).
	*/
	records, err := s.table.GetRecords().
		PageSize(20).
		WithFilterFormula(queryString).
		WithSort(struct {
			FieldName string
			Direction string
		}{FieldName: LASTMODIFIED, Direction: "asc"}).
		WithOffset(s.position.Offset).
		Do()
	if err != nil {
		return fmt.Errorf("error while getting records")
	}

	s.currentPageRecords = records
	return nil
}

func (s *CDCIterator) HasNext(ctx context.Context) bool {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "HasNext").Logger()
	logger.Trace().Msg("HasNext()")

	if s.position.RecordSlicePos >= len(s.currentPageRecords.Records)-1 { //if end of page has been reached
		s.position.RecordSlicePos = -1

		if s.currentPageRecords.Offset == "" { //if there are no more pages left
			err := s.GetRecords(ctx)
			if err != nil {
				return false
			}
			s.position.SnapshotFinished = true
			return false
		}
		err := s.GetRecords(ctx)
		if err != nil {
			return false
		}
	}

	return true
}

func (s *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "Next").Logger()
	logger.Trace().Msg("Next()")

	RecordAction := ""

	//increments internal position
	s.position.RecordSlicePos++

	//stores the CreatedTime value of the record in ISO format
	CreatedTime, err := time.Parse("2006-01-02T15:04:05Z0700", s.currentPageRecords.Records[s.position.RecordSlicePos].CreatedTime)

	if s.position.SnapshotFinished {
		if CreatedTime.Before(s.position.LastKnownTime) { //if the record was already created before the LastKnownTime,
			RecordAction = "UPDATE" // then it must be an update
		} else {
			RecordAction = "CREATE"
		}
	} else {
		RecordAction = "SNAPSHOT"
	}

	//builds the new position
	pos, err := s.buildRecordPosition()
	if err != nil {
		return sdk.Record{}, err
	}
	//logger.Warn().Msgf("Status: %v\n", RecordAction)

	switch RecordAction {

	case "SNAPSHOT":
		return sdk.Util.Source.NewRecordSnapshot( //SNAPSHOT
			pos,
			s.buildRecordMetadata(),
			s.buildRecordKey(),
			s.buildRecordPayload(),
		), nil

	case "CREATE":
		return sdk.Util.Source.NewRecordCreate( //CREATE
			pos,
			s.buildRecordMetadata(),
			s.buildRecordKey(),
			s.buildRecordPayload(),
		), nil

	case "UPDATE":
		return sdk.Util.Source.NewRecordUpdate( //UPDATE
			pos,
			s.buildRecordMetadata(),
			s.buildRecordKey(),
			nil, //Airtable does not supply old record currentPageRecords unless local caching is used
			s.buildRecordPayload(),
		), nil

	case "DELETE":
		//todo

	default:
		return sdk.Record{}, fmt.Errorf("error whilst determinining Record Action: %w", err)
	}
	return sdk.Record{}, fmt.Errorf("record could not be built: %w", err)
}

func (s *CDCIterator) buildRecordPosition() (sdk.Position, error) {
	timePos, err := time.Parse("2/1/2006 15:04:05", s.currentPageRecords.Records[s.position.RecordSlicePos].Fields[LASTMODIFIEDSTR].(string))
	if err != nil {
		return sdk.Position{}, err
	}

	s.position.LastKnownTime = timePos
	s.position.Offset = s.currentPageRecords.Offset

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
	key := s.currentPageRecords.Records[s.position.RecordSlicePos].ID //ID of individual record
	return sdk.StructuredData{
		"RecordID": key}
}

func (s *CDCIterator) buildRecordPayload() sdk.Data {
	payload := s.currentPageRecords.Records[s.position.RecordSlicePos].Fields
	return sdk.StructuredData{"Record Payload": payload}
}
