package airtable

import (
	"context"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-airtable/config"
	"github.com/conduitio-labs/conduit-connector-airtable/iterator"

	sdk "github.com/conduitio/conduit-connector-sdk"
	airtableclient "github.com/mehanizm/airtable"
)

type Source struct {
	sdk.UnimplementedSource
	client           *airtableclient.Client
	config           config.Config
	lastPositionRead sdk.Position
	iterator         Iterator
}

func NewSource() sdk.Source {
	return &Source{}
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Source Connector...")

	SourceConfig, err := config.ParseBaseConfig(cfg)
	if err != nil {
		return fmt.Errorf("couldn't parse the source config: %w", err)
	}

	s.config = SourceConfig
	sdk.Logger(ctx).Info().Msg("Successfully configured the source connector")

	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Open").Logger()
	logger.Trace().Msg("Starting Open the Source Connector...")

	s.client = airtableclient.NewClient(s.config.APIKey)

	err := s.client.SetBaseURL(s.config.URL)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while setting the Base URL")
		return fmt.Errorf("could not set base url %w", err)
	}

	s.iterator, err = iterator.NewSnapshotIterator(ctx, s.client, s.config, pos)

	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while creating iterator")
		return fmt.Errorf("couldn't create an iterator: %w", err)
	}

	logger.Trace().Msg("Successfully Created the Source Connector")

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	logger := sdk.Logger(ctx).With().Str("Class", "Source").Str("Method", "Read").Logger()
	logger.Trace().Msg("Starting Read the Source Connector...")

	if !s.iterator.HasNext(ctx) {
		logger.Debug().Msg("No more records to read, sending sdk.ErrorBackoff...")
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		logger.Error().Stack().Err(err).Msg("Error while fetching the records")
		return sdk.Record{}, fmt.Errorf("couldn't fetch the records: %w", err)
	}
	return record, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	return nil
}
