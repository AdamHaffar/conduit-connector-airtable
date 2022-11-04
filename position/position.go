package position

import (
	"encoding/json"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"time"
)

type Position struct {
	RecordSlicePos  int
	Offset          string
	LastKnownTime   time.Time
	LastKnownRecord string
}

func (p Position) ToRecordPosition() (sdk.Position, error) {
	return json.Marshal(p)
}

func ParseRecordPosition(p sdk.Position) (Position, error) {
	if p == nil {
		return Position{}, nil
	}
	var pos Position
	err := json.Unmarshal(p, &pos)
	if err != nil {
		return Position{}, err
	}
	return pos, nil
}
