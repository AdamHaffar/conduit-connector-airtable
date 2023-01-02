.PHONY: build build-exe test download

build:
	go build -o conduit-connector-airtable cmd/conduit-connector-airtable/main.go

build-exe:
	go build GOOS=windows -o conduit-connector-airtable.exe cmd/conduit-connector-airtable/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

download:
	@echo Download go.mod dependencies
	@go mod download
