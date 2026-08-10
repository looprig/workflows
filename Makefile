.PHONY: all fmt fmt-check vet test race integration recovery harness-integration staticcheck gosec govulncheck build tools-ready check

GO ?= go
GO_FILES := $(shell GOWORK=off $(GO) list -f '{{range .GoFiles}}{{.Dir}}/{{.}} {{end}}{{range .TestGoFiles}}{{.Dir}}/{{.}} {{end}}{{range .XTestGoFiles}}{{.Dir}}/{{.}} {{end}}' ./... 2>/dev/null)

all: check

fmt:
	@if [ -n "$(GO_FILES)" ]; then GOWORK=off gofmt -w $(GO_FILES); fi

fmt-check:
	@if [ -n "$(GO_FILES)" ]; then \
		unformatted=$$(gofmt -l $(GO_FILES)); \
		if [ -n "$$unformatted" ]; then \
			echo "gofmt needed (run 'make fmt'):"; echo "$$unformatted"; exit 1; \
		fi; \
	fi

vet:
	GOWORK=off $(GO) vet ./...

test:
	GOWORK=off $(GO) test ./...

race:
	GOWORK=off $(GO) test -race ./...

integration:
	GOWORK=off $(GO) test ./... -run 'TestBridgeEndToEnd|TestBridgeRecovery'

recovery:
	GOWORK=off $(GO) test ./... -run 'TestBridgeRecovery' -count=20

# This proof uses the sibling inference module through the workspace because
# workflows intentionally does not add that test-only dependency to go.mod.
harness-integration:
	$(GO) test ./... -tags harness_integration -run '^TestHarnessSessionWorkflowActivityReplayMatchesAfterRestore$$' -count=1

tools-ready:
	@for tool in staticcheck gosec govulncheck; do \
		if ! GOWORK=off $(GO) tool $$tool -h >/dev/null 2>&1; then \
			echo "pinned Go analysis tool '$$tool' is unavailable in the module cache" >&2; \
			echo "run once from this module in a network-enabled environment:" >&2; \
			echo "  GOWORK=off go mod download all" >&2; \
			echo "then rerun:" >&2; \
			echo "  GOWORK=off make check" >&2; \
			exit 1; \
		fi; \
	done

staticcheck: tools-ready
	GOWORK=off $(GO) tool staticcheck ./...

gosec: tools-ready
	GOWORK=off $(GO) tool gosec -quiet ./...

govulncheck: tools-ready
	GOWORK=off $(GO) tool govulncheck ./...

build:
	GOWORK=off $(GO) build ./...

# check is deliberately non-mutating and keeps the gate order stable.
check: fmt-check vet test race tools-ready staticcheck gosec govulncheck build
