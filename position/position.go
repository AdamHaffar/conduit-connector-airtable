package position

import (
	"encoding/json"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Position struct {
	Key       string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (p Position) ToRecordPosition() (sdk.Position, error) {
	return json.Marshal(p)
}

func ParseRecordPosition(p sdk.Position) (Position, error) {
	if p == nil {
		// empty Position would have the fields with their default values
		return Position{}, nil
	}
	var pos Position
	err := json.Unmarshal(p, &pos)
	if err != nil {
		return Position{}, err
	}
	return pos, nil
}
