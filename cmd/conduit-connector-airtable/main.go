package main

import (
	airtable "github.com/AdamHaffar/conduit-connector-airtable"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(airtable.Connector)
}
