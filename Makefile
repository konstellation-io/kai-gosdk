.DEFAULT_GOAL := help

# AutoDoc
# -------------------------------------------------------------------------
.PHONY: help
help: ## This help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
.DEFAULT_GOAL := help

.PHONY: protos
protos: ## Generate proto files
	protoc -I proto --go_out=protos --go_opt=paths=source_relative proto/kai_nats_msg.proto

.PHONY: generate_mocks
generate_mocks: ## Generate mocks
	go generate ./...

.PHONY: tidy
tidy: ## Run golangci-lint, goimports and gofmt
	golangci-lint run ./... --config .github/.golangci.yml && goimports -w  . && gofmt -s -w -e -d .

.PHONY: tests
tests: ## Run integration and unit tests
	go test ./... -cover -coverpkg=./... --tags=unit,integration
