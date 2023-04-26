.PHONY: default

default: run

init:
	go mod download
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.52.2
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest

clean:
	rm -rf ./build
	rm -rf mocks

linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

run:
	go run main.go
