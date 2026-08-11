.PHONY: all fmt fmt-check vet test race integration recovery harness-integration harness-integration-race harness-gates-test release-checkpoint staticcheck gosec govulncheck build tools-ready vuln-db-ready dependency-check dependency-policy dependency-policy-test repository-provenance-test notices-check provenance check

GO ?= go
GO_ENV := GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly

all: check

fmt:
	@set -eu; \
	files="$$( $(GO_ENV) $(GO) list -f '{{range .GoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .TestGoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .XTestGoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}' ./... )"; \
	if [ -z "$$files" ]; then echo 'fmt: no Go files were discovered' >&2; exit 1; fi; \
	gofmt -w $$files

fmt-check:
	@set -eu; \
	files="$$( $(GO_ENV) $(GO) list -f '{{range .GoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .TestGoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}{{range .XTestGoFiles}}{{$$.Dir}}/{{.}}{{"\n"}}{{end}}' ./... )"; \
	if [ -z "$$files" ]; then echo 'fmt-check: no Go files were discovered' >&2; exit 1; fi; \
	unformatted="$$(gofmt -l $$files)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed (run 'make fmt'):" >&2; echo "$$unformatted" >&2; exit 1; \
	fi

vet:
	$(GO_ENV) $(GO) vet ./...

test:
	$(GO_ENV) $(GO) test ./...

race:
	$(GO_ENV) $(GO) test -race ./...

integration:
	$(GO_ENV) $(GO) test ./... -run 'TestBridgeEndToEnd|TestBridgeRecovery' -count=1

recovery:
	$(GO_ENV) $(GO) test ./... -run 'TestBridgeRecovery' -count=20

# These proofs use the sibling inference module through an explicit local
# replacement because the tagged tests own their deterministic fake clients.
# Proxy and checksum resolution remain disabled; a missing local workspace or
# dependency is a failure, never a skipped integration test. The selector
# deliberately covers every tagged Harness integration and fault test.
harness-integration:
	$(GO_ENV) $(GO) test ./... -tags harness_integration -run '^TestHarness' -count=1 -timeout=90s

harness-integration-race:
	$(GO_ENV) $(GO) test -race ./... -tags harness_integration -run '^TestHarness' -count=1 -timeout=180s

harness-gates-test:
	./scripts/harness-gates_test.sh

tools-ready:
	@set -eu; \
	for spec in \
		'staticcheck|honnef.co/go/tools|v0.7.0' \
		'gosec|github.com/securego/gosec/v2|v2.28.0' \
		'govulncheck|golang.org/x/vuln|v1.6.0'; do \
		name=$${spec%%|*}; rest=$${spec#*|}; module=$${rest%%|*}; expected=$${rest#*|}; \
		if ! $(GO_ENV) $(GO) tool $$name -h >/dev/null 2>&1; then \
			echo "tools-ready: declared tool '$$name' is unavailable in the offline module cache" >&2; exit 1; \
		fi; \
		if ! actual=$$($(GO_ENV) $(GO) list -m -f '{{.Version}}' "$$module"); then \
			echo "tools-ready: unable to resolve pinned module '$$module' offline" >&2; exit 1; \
		fi; \
		if [ "$$actual" != "$$expected" ]; then \
			echo "tools-ready: $$module is $$actual, want pinned $$expected" >&2; exit 1; \
		fi; \
	done

staticcheck: tools-ready
	$(GO_ENV) $(GO) tool staticcheck ./...

gosec: tools-ready
	$(GO_ENV) $(GO) tool gosec -quiet ./...

vuln-db-ready:
	@set -eu; \
	db="$${POLICY53_GOVULNDB:-$${GOVULNDB:-}}"; \
	if [ -z "$$db" ]; then \
		echo 'vuln-db-ready: POLICY53_GOVULNDB must point to a local govulncheck database' >&2; exit 1; \
	fi; \
	case "$$db" in \
		file:///*) path="$${db#file://}" ;; \
		/*) path="$$db"; db="file://$$path" ;; \
		*) echo 'vuln-db-ready: network vulnerability database URLs are forbidden; use an absolute local path or file:// URL' >&2; exit 1 ;; \
	esac; \
	if [ -L "$$path" ] || [ ! -d "$$path" ]; then \
		echo "vuln-db-ready: local database directory is unavailable or unsafe: $$path" >&2; exit 1; \
	fi; \
	if [ ! -f "$$path/index/modules.json" ] && [ ! -f "$$path/index/modules.json.gz" ]; then \
		echo "vuln-db-ready: local database index is unavailable: $$path/index/modules.json" >&2; exit 1; \
	fi

govulncheck: tools-ready vuln-db-ready
	@set -eu; \
	db="$${POLICY53_GOVULNDB:-$${GOVULNDB:-}}"; \
	case "$$db" in file:///*) ;; /*) db="file://$$db" ;; esac; \
	$(GO_ENV) $(GO) tool govulncheck -db "$$db" ./...

build:
	$(GO_ENV) $(GO) build -trimpath ./...

dependency-check:
	@set -eu; \
	if ! $(GO_ENV) $(GO) mod verify; then \
		echo 'dependency-check: module cache verification failed' >&2; exit 1; \
	fi; \
	if ! $(GO_ENV) $(GO) list -m -json all >/dev/null; then \
		echo 'dependency-check: offline module graph is unavailable' >&2; exit 1; \
	fi

dependency-policy:
	./scripts/check-dependencies.sh

dependency-policy-test:
	./scripts/check-dependencies_test.sh

repository-provenance-test:
	./scripts/repository-provenance_test.sh

notices-check:
	@test -s THIRD_PARTY_NOTICES.md

provenance:
	@set -eu; \
	root="$$(pwd -P)"; test -s "$$root/go.mod"; test -s "$$root/go.sum"; \
	provenance_dir="$${WORKFLOWS_PROVENANCE_DIR:-$$(mktemp -d "$${TMPDIR:-/tmp}/workflows-provenance.XXXXXX")}"; \
	if [ -n "$${WORKFLOWS_PROVENANCE_DIR:-}" ]; then \
		case "$$provenance_dir" in /*) ;; *) echo 'provenance: WORKFLOWS_PROVENANCE_DIR must be absolute' >&2; exit 1 ;; esac; \
		[ ! -L "$$provenance_dir" ] || { echo 'provenance: output directory must not be a symlink' >&2; exit 1; }; \
		mkdir -p -- "$$provenance_dir"; \
	fi; \
	[ -d "$$provenance_dir" ] && [ ! -L "$$provenance_dir" ] || { echo 'provenance: output directory is unsafe' >&2; exit 1; }; \
	chmod 700 "$$provenance_dir"; \
	repository_dir="$$provenance_dir/repositories"; mkdir -p -- "$$repository_dir"; [ ! -L "$$repository_dir" ] || { echo 'provenance: repository evidence directory must not be a symlink' >&2; exit 1; }; chmod 700 "$$repository_dir"; \
	if [ -n "$${WORKFLOWS_REPOSITORY_PROVENANCE_PRE:-}" ]; then \
		pre_dir="$${WORKFLOWS_REPOSITORY_PROVENANCE_PRE}"; \
		case "$$pre_dir" in /*) ;; *) echo 'provenance: WORKFLOWS_REPOSITORY_PROVENANCE_PRE must be absolute' >&2; exit 1 ;; esac; \
		[ -d "$$pre_dir" ] && [ ! -L "$$pre_dir" ] && [ -f "$$pre_dir/manifest.json" ] && [ ! -L "$$pre_dir/manifest.json" ] || { echo 'provenance: supplied pre-capture repository evidence is unavailable or unsafe' >&2; exit 1; }; \
	else \
		pre_dir="$$repository_dir/pre"; \
		WORKFLOWS_GO_BINARY="$(GO)" ./scripts/repository-provenance.sh capture --module-root "$$root" --output "$$pre_dir"; \
	fi; \
	inventory="$$provenance_dir/module-inventory.jsonl"; repeat="$$provenance_dir/module-inventory.repeat.jsonl"; verify="$$provenance_dir/mod-verify.txt"; record="$$provenance_dir/provenance.json"; repository_record="$$provenance_dir/repositories.json"; \
	for output in "$$inventory" "$$repeat" "$$verify" "$$inventory.sha256" "$$repeat.sha256" "$$record" "$$repository_record"; do \
		[ ! -e "$$output" ] && [ ! -L "$$output" ] || { echo "provenance: refusing to overwrite $$output" >&2; exit 1; }; \
	done; \
	$(MAKE) --no-print-directory repository-provenance-test harness-gates-test dependency-check dependency-policy notices-check harness-integration harness-integration-race; \
	if ! $(GO_ENV) $(GO) mod verify >"$$verify" 2>&1; then echo 'provenance: go mod verify failed' >&2; exit 1; fi; \
	printf 'command=go mod verify\nresult=PASS\n' >>"$$verify"; \
	if ! $(GO_ENV) $(GO) list -m -json all >"$$inventory"; then echo 'provenance: offline module inventory failed' >&2; exit 1; fi; \
	if ! jq -e -s 'length > 0 and all(.[]; (.Path | type == "string" and length > 0) and ((.Error // null) == null))' "$$inventory" >/dev/null; then echo 'provenance: module inventory is malformed' >&2; exit 1; fi; \
	if ! $(GO_ENV) $(GO) list -m -json all >"$$repeat"; then echo 'provenance: repeated offline module inventory failed' >&2; exit 1; fi; \
	if ! cmp -s "$$inventory" "$$repeat"; then echo 'provenance: repeated module inventory differs' >&2; exit 1; fi; \
	if [ -n "$${WORKFLOWS_PROVENANCE_EXPECTED:-}" ]; then \
		expected="$${WORKFLOWS_PROVENANCE_EXPECTED}"; \
		case "$$expected" in /*) ;; *) echo 'provenance: WORKFLOWS_PROVENANCE_EXPECTED must be absolute' >&2; exit 1 ;; esac; \
		[ -f "$$expected" ] && [ ! -L "$$expected" ] || { echo 'provenance: expected evidence is unavailable or unsafe' >&2; exit 1; }; \
		cmp -s "$$inventory" "$$expected" || { echo 'provenance: module inventory does not match expected evidence' >&2; exit 1; }; \
	fi; \
	post_dir="$$repository_dir/post"; \
	WORKFLOWS_GO_BINARY="$(GO)" ./scripts/repository-provenance.sh capture --module-root "$$root" --output "$$post_dir"; \
	WORKFLOWS_GO_BINARY="$(GO)" ./scripts/repository-provenance.sh verify --pre "$$pre_dir/manifest.json" --post "$$post_dir/manifest.json" --output "$$repository_record"; \
	if command -v shasum >/dev/null 2>&1; then \
		shasum -a 256 "$$inventory" >"$$inventory.sha256"; shasum -a 256 "$$repeat" >"$$repeat.sha256"; \
		inventory_sha256="$$(shasum -a 256 "$$inventory" | awk '{print $$1}')"; repeat_sha256="$$(shasum -a 256 "$$repeat" | awk '{print $$1}')"; verify_sha256="$$(shasum -a 256 "$$verify" | awk '{print $$1}')"; repository_sha256="$$(shasum -a 256 "$$repository_record" | awk '{print $$1}')"; \
	elif command -v sha256sum >/dev/null 2>&1; then \
		sha256sum "$$inventory" >"$$inventory.sha256"; sha256sum "$$repeat" >"$$repeat.sha256"; \
		inventory_sha256="$$(sha256sum "$$inventory" | awk '{print $$1}')"; repeat_sha256="$$(sha256sum "$$repeat" | awk '{print $$1}')"; verify_sha256="$$(sha256sum "$$verify" | awk '{print $$1}')"; repository_sha256="$$(sha256sum "$$repository_record" | awk '{print $$1}')"; \
	else echo 'provenance: shasum or sha256sum is required' >&2; exit 1; fi; \
	go_version="$$($(GO_ENV) $(GO) version)"; \
	jq -S -n \
		--slurpfile repository_evidence "$$repository_record" \
		--arg module 'github.com/looprig/workflows' \
		--arg go_binary '$(GO)' \
		--arg go_env '$(GO_ENV)' \
		--arg go_version "$$go_version" \
		--arg verify_command '$(GO_ENV) $(GO) mod verify' \
		--arg inventory_command '$(GO_ENV) $(GO) list -m -json all' \
		--arg inventory_path 'module-inventory.jsonl' \
		--arg inventory_sha256 "$$inventory_sha256" \
		--arg repeat_path 'module-inventory.repeat.jsonl' \
		--arg repeat_sha256 "$$repeat_sha256" \
		--arg verify_path 'mod-verify.txt' \
		--arg verify_sha256 "$$verify_sha256" \
		--arg repository_path 'repositories.json' \
		--arg repository_sha256 "$$repository_sha256" \
		--arg harness_target 'harness-integration' \
		--arg harness_race_target 'harness-integration-race' \
		--arg harness_test_pattern '^TestHarness' \
		--arg harness_test_scope 'all tagged Harness integration and fault tests' \
		--arg harness_command '$(GO_ENV) $(GO) test ./... -tags harness_integration -run ^TestHarness -count=1 -timeout=90s' \
		--arg harness_race_command '$(GO_ENV) $(GO) test -race ./... -tags harness_integration -run ^TestHarness -count=1 -timeout=180s' \
		--arg build_command "$${WORKFLOWS_BUILD_COMMAND:-}" \
		--arg build_flags "$${WORKFLOWS_BUILD_FLAGS:-}" \
		--arg build_output "$${WORKFLOWS_BUILD_OUTPUT:-}" \
		'{schema_version: 5, module: $$module, target: "provenance", commands: {go_mod_verify: $$verify_command, module_inventory: $$inventory_command}, toolchain: {binary: $$go_binary, environment: $$go_env, version: $$go_version}, module_inventory: {path: $$inventory_path, sha256: $$inventory_sha256, repeat_path: $$repeat_path, repeat_sha256: $$repeat_sha256}, module_verification: {path: $$verify_path, sha256: $$verify_sha256}, repository_provenance: {path: $$repository_path, sha256: $$repository_sha256, evidence: $$repository_evidence[0]}, harness_integration: {required: true, test_pattern: $$harness_test_pattern, test_scope: $$harness_test_scope, normal: {target: $$harness_target, tag: "harness_integration", result: "PASS", command: $$harness_command}, race: {target: $$harness_race_target, tag: "harness_integration", result: "PASS", command: $$harness_race_command}}, build: {command: $$build_command, flags: $$build_flags, output: $$build_output}}' \
		>"$$record"; \
	printf 'provenance: retained and compared module evidence at %s\n' "$$provenance_dir"

# The wrapper captures all Workflows and local replacement repositories before
# any release gate, then delegates the dependency and tagged Harness gates to
# provenance. It rejects dirty/unavailable/drifted repositories before a
# release checkpoint can report success. Provenance runs every tagged Harness
# integration and fault test in both ordinary and race modes.
release-checkpoint:
	./scripts/release-checkpoint.sh

# check is deliberately non-mutating and keeps every required local gate
# explicit. Network-backed vulnerability lookup is never an implicit fallback.
check: fmt-check vet test race integration tools-ready staticcheck gosec govulncheck build provenance
