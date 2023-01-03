package airtable

import (
	airtable "github.com/conduitio-labs/conduit-connector-airtable/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        airtable.NewSource,
	NewDestination:   nil,
}
