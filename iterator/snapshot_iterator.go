package iterator

import (
	"context"
	"github.com/AdamHaffar/conduit-connector-airtable/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type SnapshotIterator struct {
}

func NewSnapshotIterator(config config.Config) (*SnapshotIterator, error) {
}

func (w *SnapshotIterator) HasNext(ctx context.Context) bool {
}

func (w *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	return nil
}
func (w *SnapshotIterator) Stop() {
}
