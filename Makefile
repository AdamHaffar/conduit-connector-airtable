.PHONY: build test

build:
	go build -o conduit-connector-airtable cmd/conduit-connector-airtable/main.go

test:
	go test $(GOTEST_FLAGS) -v -race ./...
