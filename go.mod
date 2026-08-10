module github.com/looprig/workflows

go 1.26.4

tool (
	github.com/securego/gosec/v2/cmd/gosec
	golang.org/x/vuln/cmd/govulncheck
	honnef.co/go/tools/cmd/staticcheck
)

require (
	github.com/securego/gosec/v2 v2.28.0
	golang.org/x/vuln v1.6.0
	honnef.co/go/tools v0.7.0
)

// Local replacements make unreleased sibling modules explicit when this
// repository is checked with GOWORK=off during development.
replace (
	github.com/looprig/flow => ../flow
	github.com/looprig/flow/store => ../flow/store
	github.com/looprig/harness => ../harness
	github.com/looprig/inference => ../inference
	github.com/looprig/sandbox => ../sandbox
	github.com/looprig/storage => ../storage
)
