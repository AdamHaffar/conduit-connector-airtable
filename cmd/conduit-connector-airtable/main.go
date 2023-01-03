package main

import (
	airtable "github.com/conduitio-labs/conduit-connector-airtable"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(airtable.Connector)
}
