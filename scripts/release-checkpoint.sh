#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
readonly REPOSITORY_HELPER="$SCRIPT_DIR/repository-provenance.sh"

fail() {
	printf 'release-checkpoint: %s\n' "$*" >&2
	exit 1
}

absolute_path() {
	local path=$1
	[[ "$path" == /* && "$path" != *$'\n'* && "$path" != *$'\r'* ]]
}

provenance_dir=${WORKFLOWS_PROVENANCE_DIR:-}
if [[ -z "$provenance_dir" ]]; then
	provenance_dir=$(mktemp -d "${TMPDIR:-/tmp}/workflows-provenance.XXXXXX")
else
	absolute_path "$provenance_dir" || fail 'WORKFLOWS_PROVENANCE_DIR must be an absolute, single-line path'
	[[ ! -L "$provenance_dir" ]] || fail 'WORKFLOWS_PROVENANCE_DIR must not be a symlink'
	mkdir -p -- "$provenance_dir"
fi
[[ -d "$provenance_dir" && ! -L "$provenance_dir" ]] || fail 'provenance output directory is unavailable or unsafe'
chmod 700 "$provenance_dir"

make_bin=${WORKFLOWS_MAKE:-make}
command -v -- "$make_bin" >/dev/null 2>&1 || fail "make is unavailable: $make_bin"

repository_pre="$provenance_dir/repositories/pre"
WORKFLOWS_GO_BINARY="${WORKFLOWS_GO_BINARY:-go}" \
	"$REPOSITORY_HELPER" capture --module-root "$ROOT" --output "$repository_pre"

# These gates run only after the pre-capture. The provenance target then runs
# dependency, notice, and both tagged Harness restore gates before taking its
# post-capture and writing provenance.json.
"$make_bin" --no-print-directory -C "$ROOT" \
	fmt-check vet test race integration tools-ready staticcheck gosec build

WORKFLOWS_PROVENANCE_DIR="$provenance_dir" \
WORKFLOWS_REPOSITORY_PROVENANCE_PRE="$repository_pre" \
	"$make_bin" --no-print-directory -C "$ROOT" provenance

printf 'release-checkpoint: PASS; retained provenance at %s\n' "$provenance_dir"
