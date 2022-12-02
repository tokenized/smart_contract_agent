BUILD_DATE = `date +%FT%T%z`
BUILD_USER = $(USER)@`hostname`
VERSION = `git describe --tags`

# command to build and run on the local OS.
GO_BUILD = go build

GO_DIST = CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO_BUILD) -a -tags netgo -ldflags "-w -X main.buildVersion=$(VERSION) -X main.buildDate=$(BUILD_DATE) -X main.buildUser=$(BUILD_USER)"

all: clean deps tools test

clean:
	go clean -testcache

deps:
	go get -t ./...

tools:
	go get golang.org/x/tools/cmd/goimports

prepare:
	mkdir -p tmp dist

test: prepare
	go test -coverprofile=tmp/coverage.out ./...

test-race:
	go test -race ./...

bench:
	go test -bench . ./...

dist-cli:
	$(GO_DIST) -o dist/sca_client cmd/client/main.go
