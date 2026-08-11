#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
readonly GATE="$SCRIPT_DIR/check-dependencies.sh"
readonly LOCK="$ROOT/../policy53/scripts/license-scanner.lock.json"

for required in jq go; do
	command -v -- "$required" >/dev/null 2>&1 || {
		printf 'mocked dependency-policy tests: required tool is unavailable: %s\n' "$required" >&2
		exit 1
	}
done

hash_file() {
	shasum -a 256 -- "$1" | awk '{print $1}'
}

hash_text() {
	printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
}

readonly TMP_DIR="$(cd -- "$(mktemp -d "${TMPDIR:-/tmp}/workflows-dependency-policy-test.XXXXXX")" && pwd -P)"
trap 'rm -rf -- "$TMP_DIR"' EXIT
readonly BIN_DIR="$TMP_DIR/bin"
mkdir -p -- "$BIN_DIR"
cp -- "$SCRIPT_DIR/testdata/mock-go.sh" "$BIN_DIR/go"
cp -- "$SCRIPT_DIR/testdata/mock-license-scanner.sh" "$BIN_DIR/mock-license-scanner"
chmod 700 "$BIN_DIR/go" "$BIN_DIR/mock-license-scanner"

SCANNER_PATH="$BIN_DIR/mock-license-scanner"
SCANNER_SHA=$(hash_file "$SCANNER_PATH")
SOURCE_SHA=$(printf 'reviewed scanner source\n' | shasum -a 256 | awk '{print $1}')
SOURCE_REVISION=0123456789abcdef0123456789abcdef01234567
INVOCATION='["--input", "<module-inventory.jsonl>", "--output", "<license-report.json>"]'
SIGNATURE='{"status":"unsupported","key_id":null}'
jq -S -n \
	--arg source_sha "$SOURCE_SHA" \
	--arg source_revision "$SOURCE_REVISION" \
	--arg distribution_sha "$SCANNER_SHA" \
	--arg executable_sha "$SCANNER_SHA" \
	--argjson invocation "$INVOCATION" \
	--argjson signature "$SIGNATURE" \
	'{schema_version: 1, status: "reviewed", name: "policy53-license-adapter", version: "1.0.0", source: "operator-reviewed-offline-license-scanner-adapter", source_reference: "test://reviewed-license-scanner", source_revision: $source_revision, source_sha256: $source_sha, distribution_kind: "standalone-executable", distribution_sha256: $distribution_sha, executable_sha256: $executable_sha, receipt_identity: null, signature: $signature, receipt_schema_version: 2, report_schema_version: 1, digest_algorithm: "sha256", network: "denied", invocation: $invocation, required_receipt_fields: ["schema_version", "name", "version", "source", "source_reference", "source_sha256", "path", "sha256", "lock_sha256", "network"]}' \
	>"$TMP_DIR/reviewed-lock.base.json"
IDENTITY=$(hash_text "$(jq -S -c '{status, schema_version, digest_algorithm, name, version, source, source_reference, source_revision, source_sha256, distribution_kind, distribution_sha256, executable_sha256, invocation, receipt_schema_version, report_schema_version, network, signature}' "$TMP_DIR/reviewed-lock.base.json")")
jq -S --arg identity "$IDENTITY" '.receipt_identity = $identity' "$TMP_DIR/reviewed-lock.base.json" >"$TMP_DIR/reviewed-lock.json"
REVIEWED_LOCK="$TMP_DIR/reviewed-lock.json"
LOCK_SHA=$(hash_file "$REVIEWED_LOCK")
jq -S -n \
	--arg path "$SCANNER_PATH" \
	--arg digest "$SCANNER_SHA" \
	--arg lock "$LOCK_SHA" \
	--arg source_digest "$SOURCE_SHA" \
	--arg source_revision "$SOURCE_REVISION" \
	--arg distribution_digest "$SCANNER_SHA" \
	--arg executable_digest "$SCANNER_SHA" \
	--arg receipt_identity "$IDENTITY" \
	--argjson signature "$SIGNATURE" \
	--argjson invocation "$INVOCATION" \
	'{schema_version: 2, name: "policy53-license-adapter", version: "1.0.0", source: "operator-reviewed-offline-license-scanner-adapter", source_reference: "test://reviewed-license-scanner", source_revision: $source_revision, source_sha256: $source_digest, distribution_kind: "standalone-executable", distribution_sha256: $distribution_digest, executable_sha256: $executable_digest, path: $path, sha256: $digest, lock_sha256: $lock, receipt_identity: $receipt_identity, signature: $signature, network: "denied", invocation: $invocation}' \
	>"$TMP_DIR/receipt.json"

run_gate() {
	local mode=$1
	local receipt=$2
	local lock=${3:-$REVIEWED_LOCK}
	local marker=${4:-}
	local output_dir="$TMP_DIR/output-$mode-$$"
	mkdir -p -- "$output_dir"
	WORKFLOWS_LICENSE_SCANNER="mock-license-scanner" \
	WORKFLOWS_LICENSE_SCANNER_LOCK="$lock" \
	WORKFLOWS_LICENSE_SCANNER_RECEIPT="$receipt" \
	WORKFLOWS_DEPENDENCY_DIR="$output_dir" \
	WORKFLOWS_LICENSE_SCANNER_TEST_MODE="$mode" \
	WORKFLOWS_LICENSE_SCANNER_EXECUTION_MARKER="$marker" \
	PATH="$BIN_DIR:$PATH" \
		"$GATE"
}

if ! run_gate good "$TMP_DIR/receipt.json" >/dev/null; then
	printf 'mocked dependency-policy tests: valid receipt/report was rejected\n' >&2
	exit 1
fi

expect_failure() {
	local label=$1
	shift
	if "$@" >/dev/null 2>&1; then
		printf 'mocked dependency-policy tests: %s was accepted\n' "$label" >&2
		exit 1
	fi
}

jq '.name = "wrong-scanner"' "$TMP_DIR/receipt.json" >"$TMP_DIR/receipt-name.json"
jq '.path = "/tmp/not-the-configured-scanner"' "$TMP_DIR/receipt.json" >"$TMP_DIR/receipt-path.json"
jq '.sha256 = ("0" * 64)' "$TMP_DIR/receipt.json" >"$TMP_DIR/receipt-digest.json"
jq '.invocation = ["--wrong"]' "$TMP_DIR/receipt.json" >"$TMP_DIR/receipt-invocation.json"
expect_failure 'receipt identity mismatch' run_gate good "$TMP_DIR/receipt-name.json"
expect_failure 'receipt executable path mismatch' run_gate good "$TMP_DIR/receipt-path.json"
expect_failure 'receipt executable digest mismatch' run_gate good "$TMP_DIR/receipt-digest.json"
expect_failure 'receipt invocation mismatch' run_gate good "$TMP_DIR/receipt-invocation.json"
expect_failure 'report scanner identity mismatch' run_gate report-name "$TMP_DIR/receipt.json"
expect_failure 'report source digest mismatch' run_gate report-source "$TMP_DIR/receipt.json"
expect_failure 'report invocation mismatch' run_gate report-invocation "$TMP_DIR/receipt.json"
expect_failure 'report receipt identity mismatch' run_gate report-receipt "$TMP_DIR/receipt.json"
expect_failure 'unbound report' run_gate unbound-report "$TMP_DIR/receipt.json"
expect_failure 'unsupported committed scanner lock' run_gate good "$TMP_DIR/receipt.json" "$LOCK"

printf '%s\n' '# substituted scanner bytes' >>"$SCANNER_PATH"
SUBSTITUTED_MARKER="$TMP_DIR/substituted-scanner-ran"
expect_failure 'substituted scanner executable' run_gate good "$TMP_DIR/receipt.json" "$REVIEWED_LOCK" "$SUBSTITUTED_MARKER"
if [[ -e "$SUBSTITUTED_MARKER" ]]; then
	printf 'mocked dependency-policy tests: substituted scanner executed before digest rejection\n' >&2
	exit 1
fi

printf 'mocked dependency-policy tests: PASS\n'
