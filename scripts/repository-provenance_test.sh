#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
readonly HELPER="$SCRIPT_DIR/repository-provenance.sh"

for required in go jq shasum; do
	command -v -- "$required" >/dev/null 2>&1 || {
		printf 'repository provenance tests: required tool is unavailable: %s\n' "$required" >&2
		exit 1
	}
done

readonly TMP_DIR="$(cd -- "$(mktemp -d "${TMPDIR:-/tmp}/workflows-repository-provenance-test.XXXXXX")" && pwd -P)"
trap 'rm -rf -- "$TMP_DIR"' EXIT
readonly BIN_DIR="$TMP_DIR/bin"
readonly FIXTURE_ROOT="$TMP_DIR/workflows"
readonly FIXTURE_REPLACEMENT="$FIXTURE_ROOT/replacement"
mkdir -p -- "$BIN_DIR" "$FIXTURE_REPLACEMENT"

printf '%s\n' \
	'module example/workflows' \
	'go 1.26.4' \
	'' \
	'replace example/replacement => ./replacement' \
	>"$FIXTURE_ROOT/go.mod"
printf '%s\n' \
	'module example/replacement' \
	'go 1.26.4' \
	>"$FIXTURE_REPLACEMENT/go.mod"
cp -- "$SCRIPT_DIR/testdata/mock-git.sh" "$BIN_DIR/git"
chmod 700 "$BIN_DIR/git"

expect_failure() {
	local label=$1
	shift
	if "$@" >/dev/null 2>&1; then
		printf 'repository provenance tests: %s was accepted\n' "$label" >&2
		exit 1
	fi
}

run_capture() {
	local mode=$1
	local phase=$2
	local output=$3
	MOCK_GIT_MODE="$mode" \
	MOCK_GIT_PHASE="$phase" \
	MOCK_WORKFLOWS_ROOT="$FIXTURE_ROOT" \
	MOCK_REPLACEMENT_ROOT="$FIXTURE_REPLACEMENT" \
	PATH="$BIN_DIR:$PATH" \
		"$HELPER" capture --module-root "$FIXTURE_ROOT" --output "$output"
}

run_verify() {
	local pre=$1
	local post=$2
	local output=$3
	PATH="$BIN_DIR:$PATH" \
		"$HELPER" verify --pre "$pre/manifest.json" --post "$post/manifest.json" --output "$output"
}

mkdir -p -- "$TMP_DIR/clean/pre" "$TMP_DIR/clean/post"
run_capture clean pre "$TMP_DIR/clean/pre"
run_capture clean post "$TMP_DIR/clean/post"
run_verify "$TMP_DIR/clean/pre" "$TMP_DIR/clean/post" "$TMP_DIR/clean/repositories.json"
jq -e '
	.schema_version == 1
	and (.repositories | length == 2)
	and all(.repositories[];
		(.module | type == "string" and length > 0)
		and (.path | startswith("/"))
		and (.pre.head | test("^[0-9a-f]{40}$"))
		and (.pre.tree | test("^[0-9a-f]{40}$"))
		and (.pre.clean == true)
		and (.pre.status_path | startswith("/"))
		and (.pre.status_sha256 | test("^[0-9a-f]{64}$"))
		and (.post.head == .pre.head)
		and (.post.tree == .pre.tree)
		and (.post.status_sha256 == .pre.status_sha256)
	)
' "$TMP_DIR/clean/repositories.json" >/dev/null

for dirty in source replacement; do
	case_dir="$TMP_DIR/dirty-$dirty"
	mkdir -p -- "$case_dir"
	expect_failure "dirty $dirty repository" env \
		MOCK_GIT_MODE="dirty-$dirty" \
		MOCK_GIT_PHASE=pre \
		MOCK_WORKFLOWS_ROOT="$FIXTURE_ROOT" \
		MOCK_REPLACEMENT_ROOT="$FIXTURE_REPLACEMENT" \
		PATH="$BIN_DIR:$PATH" \
		"$HELPER" capture --module-root "$FIXTURE_ROOT" --output "$case_dir"
done

for unavailable in source replacement; do
	case_dir="$TMP_DIR/unavailable-$unavailable"
	mkdir -p -- "$case_dir"
	expect_failure "unavailable $unavailable repository" env \
		MOCK_GIT_MODE="unavailable-$unavailable" \
		MOCK_GIT_PHASE=pre \
		MOCK_WORKFLOWS_ROOT="$FIXTURE_ROOT" \
		MOCK_REPLACEMENT_ROOT="$FIXTURE_REPLACEMENT" \
		PATH="$BIN_DIR:$PATH" \
		"$HELPER" capture --module-root "$FIXTURE_ROOT" --output "$case_dir"
done

for drift in source replacement; do
	case_dir="$TMP_DIR/drift-$drift"
	mkdir -p -- "$case_dir/pre" "$case_dir/post"
	run_capture "drift-$drift" pre "$case_dir/pre"
	run_capture "drift-$drift" post "$case_dir/post"
	expect_failure "drifted $drift repository" \
		run_verify "$case_dir/pre" "$case_dir/post" "$case_dir/repositories.json"
done

printf 'repository provenance tests: PASS\n'
