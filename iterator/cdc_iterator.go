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

const lastmodified = "last-modified"

type CDCIterator struct {
	client   *airtableclient.Client
	data     *airtableclient.Records
	position position.Position
	table    *airtableclient.Table
}

func NewCDCIterator(ctx context.Context, client *airtableclient.Client, config config.Config, pos sdk.Position) (*CDCIterator, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "cdc_iterator").Str("Method", "NewCDCIterator").Logger()
	logger.Trace().Msg("Creating new cdc iterator")

	table := client.GetTable(config.BaseID, config.TableID)

	NewPos, err := position.ParseRecordPosition(pos)
	if err != nil {
		return &CDCIterator{}, err
	}

	if pos == nil { //if no previous position, start from time 0
		NewPos.Offset = "0"
		NewPos.LastKnownTime = time.Time{}
		NewPos.RecordSlicePos = 0
	}

	s := &CDCIterator{
		client:   client,
		data:     nil,
		position: NewPos,
		table:    table,
	}
	s.GetRecords()

	return s, nil
}

func (s *CDCIterator) GetRecords() error {
	timeString := s.position.LastKnownTime.Format("02 01 2006 15:04")
	queryString := "LAST_MODIFIED_TIME()>=DATETIME_PARSE(\"" + timeString + "\", 'D MM YYYY HH:mm')"

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
	return nil
}

func (s *CDCIterator) HasNext(ctx context.Context) bool {

	if s.position.RecordSlicePos == len(s.data.Records) { //if end of page has been reached
		if s.data.Offset == "" {
			return false
		}

		s.GetRecords()
	}

	return true
}

func (s *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {

}

func (s *CDCIterator) buildRecordPosition() (sdk.Position, error) {

}

func (s *CDCIterator) buildRecordMetadata() map[string]string {

}

func (s *CDCIterator) buildRecordKey() sdk.Data {

}

func (s *CDCIterator) buildRecordPayload() sdk.Data {

}
