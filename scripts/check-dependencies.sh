#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
cd -- "$ROOT"

export GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly

for required in go jq; do
	command -v -- "$required" >/dev/null 2>&1 || {
		printf 'workflows dependency policy: required tool is unavailable: %s\n' "$required" >&2
		exit 1
	}
done

hash_file() {
	local file=$1
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256 -- "$file" | awk '{print $1}'
		return
	fi
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum -- "$file" | awk '{print $1}'
		return
	fi
	printf 'workflows dependency policy: shasum or sha256sum is required\n' >&2
	exit 1
}

hash_text() {
	local text=$1
	if command -v shasum >/dev/null 2>&1; then
		printf '%s' "$text" | shasum -a 256 | awk '{print $1}'
		return
	fi
	if command -v sha256sum >/dev/null 2>&1; then
		printf '%s' "$text" | sha256sum | awk '{print $1}'
		return
	fi
	printf 'workflows dependency policy: shasum or sha256sum is required\n' >&2
	exit 1
}

regular_file() {
	local file=$1
	[[ -f "$file" && ! -L "$file" ]]
}

canonical_file() {
	local file=$1
	regular_file "$file" || return 1
	(cd -- "$(dirname -- "$file")" && printf '%s/%s\n' "$PWD" "$(basename -- "$file")")
}

LOCK=${WORKFLOWS_LICENSE_SCANNER_LOCK:-../policy53/scripts/license-scanner.lock.json}
SCANNER=${WORKFLOWS_LICENSE_SCANNER:-}
RECEIPT=${WORKFLOWS_LICENSE_SCANNER_RECEIPT:-}
if [[ -z "$SCANNER" || -z "$RECEIPT" || ! -f "$LOCK" || -L "$LOCK" ]]; then
	printf 'workflows dependency policy: scanner, receipt, and a reviewed scanner lock are required\n' >&2
	exit 1
fi
LOCK=$(canonical_file "$LOCK") || {
	printf 'workflows dependency policy: scanner lock is unavailable or unsafe\n' >&2
	exit 1
}

if [[ "$RECEIPT" != /* || "$RECEIPT" == *$'\n'* || "$RECEIPT" == *$'\r'* ]]; then
	printf 'workflows dependency policy: scanner receipt must be an absolute, single-line path\n' >&2
	exit 1
fi

if ! jq -e '
	type == "object"
	and (keys_unsorted | sort) == [
		"digest_algorithm", "distribution_kind", "distribution_sha256",
		"executable_sha256", "invocation", "name", "network",
		"receipt_identity", "receipt_schema_version", "report_schema_version",
		"required_receipt_fields", "schema_version", "signature", "source",
		"source_reference", "source_revision", "source_sha256", "status", "version"
	]
	and .schema_version == 1
	and (.status == "reviewed" or .status == "unsupported_no_reviewed_scanner")
	and .name == "policy53-license-adapter" and .version == "1.0.0"
	and .source == "operator-reviewed-offline-license-scanner-adapter"
	and (
		(.status == "unsupported_no_reviewed_scanner"
			and .source_reference == null
			and .source_revision == null
			and .source_sha256 == null
			and .distribution_kind == null
			and .distribution_sha256 == null
			and .executable_sha256 == null
			and .receipt_identity == null
			and .signature == {status: "unsupported", key_id: null})
		or
		(.status == "reviewed"
			and (.source_reference | type == "string" and length > 0 and (contains("\n") | not) and (contains("\r") | not))
			and (.source_revision | type == "string" and test("^[0-9a-f]{40}$"))
			and (.source_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
			and (.distribution_kind == "standalone-executable" or .distribution_kind == "archive")
			and (.distribution_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
			and (.executable_sha256 | type == "string" and test("^[0-9a-f]{64}$"))
			and (.receipt_identity | type == "string" and test("^[0-9a-f]{64}$"))
		)
	)
	and (.signature | type == "object" and (keys_unsorted | sort) == ["key_id", "status"])
	and .signature.status == "unsupported"
	and .signature.key_id == null
	and .receipt_schema_version == 2 and .report_schema_version == 1
	and .digest_algorithm == "sha256" and .network == "denied"
	and .invocation == ["--input", "<module-inventory.jsonl>", "--output", "<license-report.json>"]
	and .required_receipt_fields == [
		"schema_version", "name", "version", "source", "source_reference",
		"source_sha256", "path", "sha256", "lock_sha256", "network"
	]
' "$LOCK" >/dev/null; then
	printf 'workflows dependency policy: scanner lock is malformed\n' >&2
	exit 1
fi

if [[ "$(jq -r '.status' "$LOCK")" != reviewed ]]; then
	printf 'workflows dependency policy: no reviewed offline license scanner is pinned; current scanner lock is explicitly unsupported\n' >&2
	exit 1
fi

OUTPUT_DIR=${WORKFLOWS_DEPENDENCY_DIR:-}
REMOVE_OUTPUT=false
if [[ -z "$OUTPUT_DIR" ]]; then
	OUTPUT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/workflows-dependencies.XXXXXX")
	REMOVE_OUTPUT=true
else
	[[ "$OUTPUT_DIR" == /* && ! -L "$OUTPUT_DIR" ]] || { printf 'workflows dependency policy: output directory is unsafe\n' >&2; exit 1; }
	mkdir -p -- "$OUTPUT_DIR"
fi
chmod 700 "$OUTPUT_DIR"
cleanup() {
	if [[ "$REMOVE_OUTPUT" == true ]]; then
		rm -rf -- "$OUTPUT_DIR"
	fi
}
trap cleanup EXIT

INVENTORY="$OUTPUT_DIR/module-inventory.jsonl"
REPORT="$OUTPUT_DIR/license-report.json"
if ! go list -m -json all >"$INVENTORY"; then
	printf 'workflows dependency policy: offline module inventory failed\n' >&2
	exit 1
fi
if ! jq -e -s '
	length > 0
	and all(.[];
		(.Path | type == "string" and length > 0)
		and ((.Error // null) == null)
	)
	and ((map(.Path) | length) == (map(.Path) | unique | length))
' "$INVENTORY" >/dev/null; then
	printf 'workflows dependency policy: module inventory is malformed\n' >&2
	exit 1
fi

MODULE_PATHS="$OUTPUT_DIR/module-paths.json"
jq -e -s '[.[] | .Path, (.Replace.Path // empty)] | map(select(type == "string" and length > 0)) | unique | sort' "$INVENTORY" >"$MODULE_PATHS"

PACKAGE_GRAPH="$OUTPUT_DIR/package-import-graph.jsonl"
if ! go list -deps -json ./... >"$PACKAGE_GRAPH"; then
	printf 'workflows dependency policy: offline package import graph failed\n' >&2
	exit 1
fi
if (( $(wc -c <"$PACKAGE_GRAPH") > 32 * 1024 * 1024 )); then
	printf 'workflows dependency policy: package import graph exceeds the 32 MiB bound\n' >&2
	exit 1
fi
if ! jq -e -s '
	length > 0
	and all(.[];
		(.ImportPath | type == "string" and length > 0)
		and ((.Imports // []) | type == "array" and all(.[]; type == "string" and length > 0))
	)
' "$PACKAGE_GRAPH" >/dev/null; then
	printf 'workflows dependency policy: package import graph is malformed or incomplete\n' >&2
	exit 1
fi

# Keep this list at package-path granularity. Checking the effective package
# graph catches a forbidden capability only when it is actually reachable by
# this module, while avoiding false positives from inert go.mod entries.
readonly FORBIDDEN_PACKAGE_PREFIXES_JSON='[
	"github.com/looprig/coderig",
	"github.com/looprig/llm",
	"github.com/looprig/natsstore",
	"github.com/looprig/rclone",
	"github.com/looprig/mcp",
	"github.com/looprig/ocr",
	"github.com/looprig/browser",
	"github.com/looprig/tools/websearch",
	"github.com/looprig/tools/fetch",
	"github.com/looprig/tools/bash",
	"github.com/looprig/tools/process",
	"github.com/looprig/tools/editfile",
	"github.com/looprig/tools/writefile",
	"github.com/looprig/tools/readfile",
	"github.com/looprig/tools/grep",
	"github.com/looprig/tools/glob",
	"github.com/looprig/harness/internal/codingtool",
	"github.com/looprig/harness/internal/webtool",
	"github.com/nats-io",
	"github.com/looprig/harness/internal/delegationtool",
	"github.com/looprig/inference/codec/anthropicapi",
	"github.com/looprig/inference/codec/geminiapi",
	"github.com/looprig/inference/codec/openairesponses",
	"github.com/looprig/inference/codec/bedrockconverse",
	"github.com/anthropics/anthropic-sdk-go",
	"google.golang.org/genai",
	"github.com/openai/openai-go/v3"
]'
FORBIDDEN_PACKAGES=$(jq -r -s \
	--argjson forbidden "$FORBIDDEN_PACKAGE_PREFIXES_JSON" '
	[
		.[]
		| (.ImportPath, ((.Imports // [])[]))
		| select(type == "string")
		| . as $path
		| select(any($forbidden[]; . as $prefix | $path == $prefix or ($path | startswith($prefix + "/"))))
	]
	| unique | sort | .[]?
' "$PACKAGE_GRAPH")
if [[ -n "$FORBIDDEN_PACKAGES" ]]; then
	printf 'workflows dependency policy: forbidden package prefix is reachable in the effective graph:\n%s\n' "$FORBIDDEN_PACKAGES" >&2
	exit 1
fi

LOCK_SHA=$(hash_file "$LOCK")
SCANNER_PATH=$(command -v -- "$SCANNER" || true)
[[ -n "$SCANNER_PATH" && -x "$SCANNER_PATH" && ! -L "$SCANNER_PATH" && -f "$SCANNER_PATH" ]] || {
	printf 'workflows dependency policy: scanner executable is unavailable or unsafe\n' >&2
	exit 1
}
SCANNER_PATH=$(canonical_file "$SCANNER_PATH") || {
	printf 'workflows dependency policy: scanner executable is unavailable or unsafe\n' >&2
	exit 1
}
RECEIPT=$(canonical_file "$RECEIPT") || {
	printf 'workflows dependency policy: scanner receipt is unavailable or unsafe\n' >&2
	exit 1
}
SCANNER_SHA=$(hash_file "$SCANNER_PATH")
SCANNER_RECEIPT_SHA=$(hash_file "$RECEIPT")
SCANNER_NAME=$(jq -r '.name' "$LOCK")
SCANNER_VERSION=$(jq -r '.version' "$LOCK")
SCANNER_SOURCE=$(jq -r '.source' "$LOCK")
SCANNER_SOURCE_REFERENCE=$(jq -r '.source_reference' "$LOCK")
SCANNER_SOURCE_REVISION=$(jq -r '.source_revision' "$LOCK")
SCANNER_EXPECTED_SOURCE_DIGEST=$(jq -r '.source_sha256' "$LOCK")
SCANNER_DISTRIBUTION_KIND=$(jq -r '.distribution_kind' "$LOCK")
SCANNER_DISTRIBUTION_DIGEST=$(jq -r '.distribution_sha256' "$LOCK")
SCANNER_EXECUTABLE_DIGEST=$(jq -r '.executable_sha256' "$LOCK")
SCANNER_RECEIPT_IDENTITY=$(jq -r '.receipt_identity' "$LOCK")
SCANNER_SIGNATURE=$(jq -c '.signature' "$LOCK")
SCANNER_RECEIPT_SCHEMA=$(jq -r '.receipt_schema_version' "$LOCK")
SCANNER_REPORT_SCHEMA=$(jq -r '.report_schema_version' "$LOCK")
SCANNER_INVOCATION=$(jq -c '.invocation' "$LOCK")

SCANNER_IDENTITY_MATERIAL=$(jq -S -c '{
	status,
	schema_version,
	digest_algorithm,
	name,
	version,
	source,
	source_reference,
	source_revision,
	source_sha256,
	distribution_kind,
	distribution_sha256,
	executable_sha256,
	invocation,
	receipt_schema_version,
	report_schema_version,
	network,
	signature
}' "$LOCK")
if [[ "$(hash_text "$SCANNER_IDENTITY_MATERIAL")" != "$SCANNER_RECEIPT_IDENTITY" ]]; then
	printf 'workflows dependency policy: reviewed scanner lock has a non-canonical receipt identity\n' >&2
	exit 1
fi
if [[ "$SCANNER_SHA" != "$SCANNER_EXECUTABLE_DIGEST" ]]; then
	printf 'workflows dependency policy: configured license scanner bytes do not match the reviewed executable digest\n' >&2
	exit 1
fi
if [[ "$SCANNER_DISTRIBUTION_KIND" == standalone-executable ]]; then
	SCANNER_ACTUAL_DISTRIBUTION_DIGEST="$SCANNER_SHA"
else
	SCANNER_DISTRIBUTION=${WORKFLOWS_LICENSE_SCANNER_DISTRIBUTION:-}
	if [[ -z "$SCANNER_DISTRIBUTION" || "$SCANNER_DISTRIBUTION" != /* || "$SCANNER_DISTRIBUTION" == *$'\n'* || "$SCANNER_DISTRIBUTION" == *$'\r'* ]] || ! regular_file "$SCANNER_DISTRIBUTION"; then
		printf 'workflows dependency policy: reviewed scanner lock requires an absolute, regular distribution artifact\n' >&2
		exit 1
	fi
	SCANNER_DISTRIBUTION=$(canonical_file "$SCANNER_DISTRIBUTION")
	SCANNER_ACTUAL_DISTRIBUTION_DIGEST=$(hash_file "$SCANNER_DISTRIBUTION")
fi
if [[ "$SCANNER_ACTUAL_DISTRIBUTION_DIGEST" != "$SCANNER_DISTRIBUTION_DIGEST" ]]; then
	printf 'workflows dependency policy: configured license scanner distribution does not match the reviewed distribution digest\n' >&2
	exit 1
fi
if ! jq -e \
	--arg name "$SCANNER_NAME" \
	--arg version "$SCANNER_VERSION" \
	--arg source "$SCANNER_SOURCE" \
	--arg source_reference "$SCANNER_SOURCE_REFERENCE" \
	--arg source_revision "$SCANNER_SOURCE_REVISION" \
	--arg source_digest "$SCANNER_EXPECTED_SOURCE_DIGEST" \
	--arg distribution_kind "$SCANNER_DISTRIBUTION_KIND" \
	--arg distribution_digest "$SCANNER_DISTRIBUTION_DIGEST" \
	--arg executable_digest "$SCANNER_EXECUTABLE_DIGEST" \
	--arg receipt_identity "$SCANNER_RECEIPT_IDENTITY" \
	--arg path "$SCANNER_PATH" \
	--arg digest "$SCANNER_SHA" \
	--arg lock "$LOCK_SHA" \
	--argjson signature "$SCANNER_SIGNATURE" \
	--argjson schema "$SCANNER_RECEIPT_SCHEMA" \
	--argjson invocation "$SCANNER_INVOCATION" '
	type == "object"
	and (keys_unsorted | sort) == [
		"distribution_kind", "distribution_sha256", "executable_sha256", "invocation", "lock_sha256",
		"name", "network", "path", "receipt_identity", "schema_version", "sha256",
		"signature", "source", "source_reference", "source_revision", "source_sha256", "version"
	]
	and .schema_version == $schema
	and .name == $name
	and .version == $version
	and .source == $source
	and .source_reference == $source_reference
	and .source_revision == $source_revision
	and .source_sha256 == $source_digest
	and .distribution_kind == $distribution_kind
	and .distribution_sha256 == $distribution_digest
	and .executable_sha256 == $executable_digest
	and .path == $path
	and (.sha256 | type == "string" and . == $digest)
	and (.lock_sha256 | type == "string" and . == $lock)
	and .receipt_identity == $receipt_identity
	and .signature == $signature
	and .invocation == $invocation
	and .network == "denied"
' "$RECEIPT" >/dev/null; then
	printf 'workflows dependency policy: scanner receipt is not bound to the reviewed source, distribution, executable, receipt identity, and offline boundary\n' >&2
	exit 1
fi

export WORKFLOWS_LICENSE_SCANNER_LOCK="$LOCK"
export WORKFLOWS_LICENSE_SCANNER_LOCK_SHA256="$LOCK_SHA"
export WORKFLOWS_LICENSE_SCANNER_PATH="$SCANNER_PATH"
export WORKFLOWS_LICENSE_SCANNER_RECEIPT="$RECEIPT"
export WORKFLOWS_LICENSE_SCANNER_RECEIPT_SHA256="$SCANNER_RECEIPT_SHA"
export WORKFLOWS_LICENSE_SCANNER_RECEIPT_IDENTITY="$SCANNER_RECEIPT_IDENTITY"
export WORKFLOWS_LICENSE_SCANNER_SOURCE_REFERENCE="$SCANNER_SOURCE_REFERENCE"
export WORKFLOWS_LICENSE_SCANNER_SOURCE_REVISION="$SCANNER_SOURCE_REVISION"
export WORKFLOWS_LICENSE_SCANNER_SOURCE_SHA256="$SCANNER_EXPECTED_SOURCE_DIGEST"
export WORKFLOWS_LICENSE_SCANNER_DISTRIBUTION_KIND="$SCANNER_DISTRIBUTION_KIND"
export WORKFLOWS_LICENSE_SCANNER_DISTRIBUTION_SHA256="$SCANNER_DISTRIBUTION_DIGEST"
export WORKFLOWS_LICENSE_SCANNER_EXECUTABLE_SHA256="$SCANNER_EXECUTABLE_DIGEST"
export WORKFLOWS_LICENSE_SCANNER_INVOCATION="$SCANNER_INVOCATION"
export WORKFLOWS_LICENSE_SCANNER_SIGNATURE="$SCANNER_SIGNATURE"
if ! "$SCANNER_PATH" --input "$INVENTORY" --output "$REPORT"; then
	printf 'workflows dependency policy: license scanner failed\n' >&2
	exit 1
fi
if [[ "$(hash_file "$SCANNER_PATH")" != "$SCANNER_EXECUTABLE_DIGEST" ]]; then
	printf 'workflows dependency policy: license scanner bytes changed during execution\n' >&2
	exit 1
fi
if [[ "$(hash_file "$RECEIPT")" != "$SCANNER_RECEIPT_SHA" ]]; then
	printf 'workflows dependency policy: scanner receipt changed during execution\n' >&2
	exit 1
fi
if ! regular_file "$REPORT"; then
	printf 'workflows dependency policy: license scanner did not produce a regular report\n' >&2
	exit 1
fi
if (( $(wc -c <"$REPORT") > 8 * 1024 * 1024 )); then
	printf 'workflows dependency policy: license report exceeds the 8 MiB bound\n' >&2
	exit 1
fi
if ! jq -e -s --slurpfile expected "$MODULE_PATHS" \
	--arg name "$SCANNER_NAME" \
	--arg version "$SCANNER_VERSION" \
	--arg source "$SCANNER_SOURCE" \
	--arg source_reference "$SCANNER_SOURCE_REFERENCE" \
	--arg source_revision "$SCANNER_SOURCE_REVISION" \
	--arg source_digest "$SCANNER_EXPECTED_SOURCE_DIGEST" \
	--arg distribution_kind "$SCANNER_DISTRIBUTION_KIND" \
	--arg distribution_digest "$SCANNER_DISTRIBUTION_DIGEST" \
	--arg executable_digest "$SCANNER_EXECUTABLE_DIGEST" \
	--arg receipt_identity "$SCANNER_RECEIPT_IDENTITY" \
	--arg path "$SCANNER_PATH" \
	--arg digest "$SCANNER_SHA" \
	--arg receipt_digest "$SCANNER_RECEIPT_SHA" \
	--arg lock "$LOCK_SHA" \
	--argjson signature "$SCANNER_SIGNATURE" \
	--argjson schema "$SCANNER_REPORT_SCHEMA" \
	--argjson invocation "$SCANNER_INVOCATION" \
	--argjson approved '[
		"Apache-2.0", "BSD-0-Clause", "BSD-2-Clause", "BSD-3-Clause",
		"CC0-1.0", "ISC", "MIT", "MPL-2.0", "Public-Domain",
		"Unicode-3.0", "Unicode-DFS-2016", "Zlib"
	]' '
	length == 1
	and (.[0] | type == "object")
	and (.[0] | (keys_unsorted | sort) == ["modules", "network", "scanner", "schema_version"])
	and .[0].schema_version == $schema
	and .[0].network == "denied"
	and (.[0].scanner | type == "object")
	and (.[0].scanner | (keys_unsorted | sort) == [
		"distribution_kind", "distribution_sha256", "executable_sha256", "invocation", "lock_sha256",
		"name", "network", "path", "receipt_identity", "receipt_sha256", "sha256",
		"signature", "source", "source_reference", "source_revision", "source_sha256", "version"
	])
	and .[0].scanner.name == $name
	and .[0].scanner.version == $version
	and .[0].scanner.source == $source
	and .[0].scanner.source_reference == $source_reference
	and .[0].scanner.source_revision == $source_revision
	and .[0].scanner.source_sha256 == $source_digest
	and .[0].scanner.distribution_kind == $distribution_kind
	and .[0].scanner.distribution_sha256 == $distribution_digest
	and .[0].scanner.executable_sha256 == $executable_digest
	and .[0].scanner.path == $path
	and .[0].scanner.receipt_identity == $receipt_identity
	and .[0].scanner.receipt_sha256 == $receipt_digest
	and .[0].scanner.sha256 == $digest
	and .[0].scanner.lock_sha256 == $lock
	and .[0].scanner.invocation == $invocation
	and .[0].scanner.signature == $signature
	and .[0].scanner.network == "denied"
	and (.[0].modules | type == "array" and length > 0)
	and (.[0].modules | length == ($expected[0] | length))
	and ((.[0].modules | map(.module) | length) == (.[0].modules | map(.module) | unique | length))
	and ((.[0].modules | map(.module) | unique | sort) == ($expected[0]))
	and all(.[0].modules[];
		(type == "object")
		and (.module | type == "string" and length > 0)
		and (.license | type == "string" and length > 0)
		and (.license as $license | any($approved[]; . == $license))
		and (.network == "denied")
	)
' "$REPORT" >/dev/null; then
	printf 'workflows dependency policy: license report is malformed or incomplete\n' >&2
	exit 1
fi
printf 'workflows dependency policy: PASS\n'
