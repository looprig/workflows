#!/usr/bin/env bash
set -Eeuo pipefail

input=""
output=""
while (($#)); do
	case "$1" in
		--input) input=$2; shift 2 ;;
		--output) output=$2; shift 2 ;;
		*) exit 2 ;;
	esac
done
[[ -n "$input" && -n "$output" ]]
if [[ -n "${WORKFLOWS_LICENSE_SCANNER_EXECUTION_MARKER:-}" ]]; then
	: >"$WORKFLOWS_LICENSE_SCANNER_EXECUTION_MARKER"
fi

mode=${WORKFLOWS_LICENSE_SCANNER_TEST_MODE:-good}
if [[ "$mode" == unbound-report ]]; then
	jq -S -n --slurpfile modules "$input" \
		'{schema_version: 1, network: "denied", modules: ($modules | map({module: .Path, license: "MIT", network: "denied"}))}' \
		>"$output"
	exit 0
fi

jq -S -n \
	--slurpfile receipt "$WORKFLOWS_LICENSE_SCANNER_RECEIPT" \
	--slurpfile modules "$input" \
	--arg mode "$mode" \
	'
	($receipt[0] | {name, version, source, source_reference, source_revision, source_sha256, distribution_kind, distribution_sha256, executable_sha256, path, sha256, lock_sha256, receipt_identity, signature, network, invocation, receipt_sha256: ($ENV.WORKFLOWS_LICENSE_SCANNER_RECEIPT_SHA256 // "")}) as $scanner
	| ($modules | map({module: .Path, license: "MIT", network: "denied"})) as $rows
	| (if $mode == "report-name" then $scanner.name = "wrong-scanner"
	   elif $mode == "report-source" then $scanner.source_sha256 = ("0" * 64)
	   elif $mode == "report-invocation" then $scanner.invocation = ["--wrong"]
	   elif $mode == "report-receipt" then $scanner.receipt_sha256 = ("0" * 64)
	   else $scanner
	   end) as $bound
	| {schema_version: 1, network: "denied", scanner: $bound, modules: $rows}
	' >"$output"
