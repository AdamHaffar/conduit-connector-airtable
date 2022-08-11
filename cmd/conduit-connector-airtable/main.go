package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(sdk.Connector{})
}
