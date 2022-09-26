package airtable

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "airtable",
		Summary:     "An airtable source plugin for Conduit.",
		Description: `tbd`,
		Version:     "v0.1.0",
		Author:      "Meroxa, Inc.",
	}
}
