#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly DEFAULT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
readonly DEFAULT_MAX_STATUS_BYTES=65536

usage() {
	cat >&2 <<'EOF'
usage:
  repository-provenance.sh capture --module-root DIR --output DIR
  repository-provenance.sh verify --pre MANIFEST --post MANIFEST --output FILE
EOF
}

fail() {
	printf 'repository-provenance: %s\n' "$*" >&2
	exit 1
}

regular_file() {
	local file=$1
	[[ -f "$file" && ! -L "$file" ]]
}

canonical_dir() {
	local dir=$1
	[[ -d "$dir" && ! -L "$dir" ]] || return 1
	(cd -P -- "$dir" && pwd -P)
}

canonical_file() {
	local file=$1
	regular_file "$file" || return 1
	(cd -P -- "$(dirname -- "$file")" && printf '%s/%s\n' "$PWD" "$(basename -- "$file")")
}

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
	fail 'shasum or sha256sum is required'
}

absolute_path() {
	local path=$1
	[[ "$path" == /* && "$path" != *$'\n'* && "$path" != *$'\r'* ]]
}

prepare_output_dir() {
	local output=$1
	absolute_path "$output" || fail "output directory must be an absolute, single-line path: $output"
	if [[ -e "$output" || -L "$output" ]]; then
		[[ ! -L "$output" && -d "$output" ]] || fail "output directory is unavailable or unsafe: $output"
	else
		mkdir -p -- "$output"
	fi
	[[ ! -L "$output" && -d "$output" ]] || fail "output directory is unavailable or unsafe: $output"
	chmod 700 "$output"
	mkdir -p -- "$output/status"
	[[ ! -L "$output/status" && -d "$output/status" ]] || fail "status directory is unavailable or unsafe: $output/status"
	chmod 700 "$output/status"
}

ensure_new_file() {
	local file=$1
	[[ ! -e "$file" && ! -L "$file" ]] || fail "refusing to overwrite $file"
}

max_status_bytes() {
	local value=${WORKFLOWS_REPOSITORY_STATUS_MAX_BYTES:-$DEFAULT_MAX_STATUS_BYTES}
	[[ "$value" =~ ^[0-9]+$ ]] || fail 'WORKFLOWS_REPOSITORY_STATUS_MAX_BYTES must be a decimal integer'
	(( value > 0 && value <= 1048576 )) || fail 'WORKFLOWS_REPOSITORY_STATUS_MAX_BYTES must be between 1 and 1048576'
	printf '%s\n' "$value"
}

capture_repository() {
	local index=$1
	local module=$2
	local module_path=$3
	local output=$4
	local max_bytes=$5

	module_path=$(canonical_dir "$module_path") || fail "local replacement is unavailable or unsafe: $module_path"
	local module_go_mod_path="$module_path/go.mod"
	module_go_mod_path=$(canonical_file "$module_go_mod_path") || fail "module go.mod is unavailable or unsafe: $module_go_mod_path"
	local module_go_mod_sha256
	module_go_mod_sha256=$(hash_file "$module_go_mod_path")

	local repo_root
	repo_root=$(git -C "$module_path" rev-parse --show-toplevel 2>/dev/null) || fail "Git repository is unavailable: $module_path"
	repo_root=$(canonical_dir "$repo_root") || fail "Git repository root is unavailable or unsafe: $repo_root"
	case "$module_path" in
		"$repo_root"|"$repo_root"/*) ;;
		*) fail "module path is outside its Git repository: $module_path" ;;
	esac

	local relative_path
	relative_path=$(git -C "$module_path" rev-parse --show-prefix 2>/dev/null) || fail "Git prefix is unavailable: $module_path"
	relative_path=${relative_path%/}
	[[ "$relative_path" != *$'\n'* && "$relative_path" != *$'\r'* ]] || fail "Git prefix is malformed: $module_path"

	local head tree module_tree
	head=$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}' 2>/dev/null) || fail "Git HEAD is unavailable: $repo_root"
	tree=$(git -C "$repo_root" rev-parse --verify 'HEAD^{tree}' 2>/dev/null) || fail "Git tree is unavailable: $repo_root"
	if [[ -z "$relative_path" ]]; then
		module_tree=$tree
	else
		module_tree=$(git -C "$repo_root" rev-parse --verify "HEAD:$relative_path" 2>/dev/null) || fail "module tree is unavailable: $module_path"
	fi
	[[ "$head" =~ ^[0-9a-f]{40}$ ]] || fail "Git HEAD is malformed: $repo_root"
	[[ "$tree" =~ ^[0-9a-f]{40}$ ]] || fail "Git tree is malformed: $repo_root"
	[[ "$module_tree" =~ ^[0-9a-f]{40}$ ]] || fail "module tree is malformed: $module_path"

	local status_path="$output/status/$index.status"
	local evidence_path="$output/status/$index.evidence.json"
	ensure_new_file "$status_path"
	ensure_new_file "$evidence_path"
	if ! LC_ALL=C git -C "$repo_root" status --porcelain=v1 --untracked-files=all -- . >"$status_path"; then
		fail "Git status is unavailable: $repo_root"
	fi
	local status_bytes
	status_bytes=$(wc -c <"$status_path" | awk '{print $1}')
	[[ "$status_bytes" =~ ^[0-9]+$ ]] || fail "Git status byte count is malformed: $repo_root"
	(( status_bytes <= max_bytes )) || fail "Git status exceeds the $max_bytes-byte bound: $repo_root"
	local status_sha256
	status_sha256=$(hash_file "$status_path")
	local clean=true
	if (( status_bytes != 0 )); then
		clean=false
		dirty_modules+=("$module")
	fi

	jq -S -n \
		--arg module "$module" \
		--arg path "$module_path" \
		--arg repo_root "$repo_root" \
		--arg relative_path "${relative_path:-.}" \
		--arg module_go_mod_path "$module_go_mod_path" \
		--arg module_go_mod_sha256 "$module_go_mod_sha256" \
		--arg head "$head" \
		--arg tree "$tree" \
		--arg module_tree "$module_tree" \
		--arg status_path "$status_path" \
		--arg status_sha256 "$status_sha256" \
		--argjson status_bytes "$status_bytes" \
		--argjson clean "$clean" \
		'{schema_version: 1, module: $module, path: $path, repo_root: $repo_root, relative_path: $relative_path, module_go_mod_path: $module_go_mod_path, module_go_mod_sha256: $module_go_mod_sha256, head: $head, tree: $tree, module_tree: $module_tree, clean: $clean, status_path: $status_path, status_bytes: $status_bytes, status_sha256: $status_sha256}' \
		>"$evidence_path"
	local evidence_sha256
	evidence_sha256=$(hash_file "$evidence_path")

	jq -S -n \
		--arg module "$module" \
		--arg path "$module_path" \
		--arg repo_root "$repo_root" \
		--arg relative_path "${relative_path:-.}" \
		--arg module_go_mod_path "$module_go_mod_path" \
		--arg module_go_mod_sha256 "$module_go_mod_sha256" \
		--arg head "$head" \
		--arg tree "$tree" \
		--arg module_tree "$module_tree" \
		--arg status_path "$status_path" \
		--arg status_sha256 "$status_sha256" \
		--arg evidence_path "$evidence_path" \
		--arg evidence_sha256 "$evidence_sha256" \
		--argjson status_bytes "$status_bytes" \
		--argjson clean "$clean" \
		'{schema_version: 1, module: $module, path: $path, repo_root: $repo_root, relative_path: $relative_path, module_go_mod_path: $module_go_mod_path, module_go_mod_sha256: $module_go_mod_sha256, head: $head, tree: $tree, module_tree: $module_tree, clean: $clean, status_path: $status_path, status_bytes: $status_bytes, status_sha256: $status_sha256, evidence_path: $evidence_path, evidence_sha256: $evidence_sha256}' \
		>>"$RECORDS_FILE"
}

capture() {
	local module_root=${DEFAULT_ROOT}
	local output=
	while (($#)); do
		case $1 in
			--module-root)
				(($# >= 2)) || { usage; exit 2; }
				module_root=$2
				shift 2
				;;
			--output)
				(($# >= 2)) || { usage; exit 2; }
				output=$2
				shift 2
				;;
			*)
				usage
				exit 2
				;;
		esac
	done
	[[ -n "$output" ]] || { usage; exit 2; }
	module_root=$(canonical_dir "$module_root") || fail "module root is unavailable or unsafe: $module_root"
	output=$(if [[ "$output" == /* ]]; then printf '%s\n' "$output"; else printf '%s/%s\n' "$PWD" "$output"; fi)
	prepare_output_dir "$output"
	ensure_new_file "$output/manifest.json"
	ensure_new_file "$output/go-mod-edit.json"
	ensure_new_file "$output/records.jsonl"
	local max_bytes
	max_bytes=$(max_status_bytes)

	if ! (cd -- "$module_root" && GOWORK=off GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-mod=readonly "${WORKFLOWS_GO_BINARY:-go}" mod edit -json) >"$output/go-mod-edit.json"; then
		fail "unable to parse go.mod with go mod edit"
	fi
	if ! jq -e '
		type == "object"
		and (.Module | type == "object")
		and (.Module.Path | type == "string" and length > 0)
		and ((.Replace // []) | type == "array")
	' "$output/go-mod-edit.json" >/dev/null; then
		fail 'go mod edit output is malformed'
	fi
	local module
	module=$(jq -r '.Module.Path' "$output/go-mod-edit.json")
	local root_go_mod="$module_root/go.mod"
	root_go_mod=$(canonical_file "$root_go_mod") || fail "module go.mod is unavailable or unsafe: $root_go_mod"
	local root_go_mod_sha256
	root_go_mod_sha256=$(hash_file "$root_go_mod")

	RECORDS_FILE="$output/records.jsonl"
	dirty_modules=()
	capture_repository 000 "$module" "$module_root" "$output" "$max_bytes"
	local index=1
	local replacement_module replacement_path
	while IFS=$'\t' read -r replacement_module replacement_path; do
		[[ -n "$replacement_module" && -n "$replacement_path" ]] || continue
		if [[ "$replacement_path" == /* ]]; then
			replacement_path=$replacement_path
		else
			replacement_path="$module_root/$replacement_path"
		fi
		printf -v index_name '%03d' "$index"
		capture_repository "$index_name" "$replacement_module" "$replacement_path" "$output" "$max_bytes"
		((index += 1))
	done < <(jq -r '
		(.Replace // [])[]
		| select((.Old.Path | type == "string" and length > 0)
			and (.New.Path | type == "string" and length > 0)
			and ((.New.Version // "") == ""))
		| [.Old.Path, .New.Path]
		| @tsv
	' "$output/go-mod-edit.json")

	if ((${#dirty_modules[@]} != 0)); then
		fail "repository is dirty: ${dirty_modules[*]}"
	fi
	jq -S -s \
		--arg module_root "$module_root" \
		--arg go_mod_path "$root_go_mod" \
		--arg go_mod_sha256 "$root_go_mod_sha256" \
		--argjson max_status_bytes "$max_bytes" \
		'{schema_version: 1, module_root: $module_root, go_mod_path: $go_mod_path, go_mod_sha256: $go_mod_sha256, max_status_bytes: $max_status_bytes, repositories: (sort_by(.module, .path))}' \
		"$RECORDS_FILE" >"$output/manifest.json"
	chmod 600 "$output/manifest.json" "$output/go-mod-edit.json" "$output/records.jsonl"
	printf 'repository-provenance: captured %s repository records at %s\n' "$(jq -r '.repositories | length' "$output/manifest.json")" "$output/manifest.json"
}

validate_manifest() {
	local manifest=$1
	regular_file "$manifest" || fail "manifest is unavailable or unsafe: $manifest"
	if ! jq -e '
		def sha256: type == "string" and test("^[0-9a-f]{64}$");
		def gitsha: type == "string" and test("^[0-9a-f]{40}$");
		type == "object"
		and .schema_version == 1
		and (.module_root | type == "string" and startswith("/"))
		and (.go_mod_path | type == "string" and startswith("/"))
		and (.go_mod_sha256 | sha256)
		and (.max_status_bytes | type == "number" and . > 0 and . <= 1048576)
		and (.repositories | type == "array" and length > 0)
		and ((.repositories | map(.module) | length) == (.repositories | map(.module) | unique | length))
		and all(.repositories[];
			type == "object"
			and (.schema_version == 1)
			and (.module | type == "string" and length > 0)
			and (.path | type == "string" and startswith("/"))
			and (.repo_root | type == "string" and startswith("/"))
			and (.relative_path | type == "string" and length > 0)
			and (.module_go_mod_path | type == "string" and startswith("/"))
			and (.module_go_mod_sha256 | sha256)
			and (.head | gitsha) and (.tree | gitsha) and (.module_tree | gitsha)
			and (.clean | type == "boolean")
			and (.status_path | type == "string" and startswith("/"))
			and (.status_bytes | type == "number" and . >= 0 and . <= 1048576)
			and (.status_sha256 | sha256)
			and (.evidence_path | type == "string" and startswith("/"))
			and (.evidence_sha256 | sha256)
		)
	' "$manifest" >/dev/null; then
		fail "manifest is malformed: $manifest"
	fi
}

verify_evidence_files() {
	local manifest=$1
	local root_go_mod root_go_mod_sha256
	root_go_mod=$(jq -r '.go_mod_path' "$manifest")
	root_go_mod_sha256=$(jq -r '.go_mod_sha256' "$manifest")
	regular_file "$root_go_mod" || fail "manifest go.mod evidence is unavailable: $root_go_mod"
	[[ "$(hash_file "$root_go_mod")" == "$root_go_mod_sha256" ]] || fail "module go.mod drifted after capture: $root_go_mod"

	local entry module_go_mod_path module_go_mod_sha256 status_path evidence_path status_sha256 evidence_sha256 status_bytes
	while IFS= read -r entry; do
		module_go_mod_path=$(jq -r '.module_go_mod_path' <<<"$entry")
		module_go_mod_sha256=$(jq -r '.module_go_mod_sha256' <<<"$entry")
		status_path=$(jq -r '.status_path' <<<"$entry")
		evidence_path=$(jq -r '.evidence_path' <<<"$entry")
		status_sha256=$(jq -r '.status_sha256' <<<"$entry")
		evidence_sha256=$(jq -r '.evidence_sha256' <<<"$entry")
		status_bytes=$(jq -r '.status_bytes' <<<"$entry")
		regular_file "$module_go_mod_path" || fail "replacement go.mod evidence is unavailable: $module_go_mod_path"
		[[ "$(hash_file "$module_go_mod_path")" == "$module_go_mod_sha256" ]] || fail "replacement module go.mod drifted after capture: $module_go_mod_path"
		regular_file "$status_path" || fail "status evidence is unavailable: $status_path"
		regular_file "$evidence_path" || fail "repository evidence is unavailable: $evidence_path"
		[[ "$(wc -c <"$status_path" | awk '{print $1}')" == "$status_bytes" ]] || fail "status evidence byte count changed: $status_path"
		[[ "$(hash_file "$status_path")" == "$status_sha256" ]] || fail "status evidence was modified: $status_path"
		[[ "$(hash_file "$evidence_path")" == "$evidence_sha256" ]] || fail "repository evidence was modified: $evidence_path"
		if ! jq -e \
			--arg module "$(jq -r '.module' <<<"$entry")" \
			--arg path "$(jq -r '.path' <<<"$entry")" \
			--arg repo_root "$(jq -r '.repo_root' <<<"$entry")" \
			--arg relative_path "$(jq -r '.relative_path' <<<"$entry")" \
			--arg module_go_mod_path "$module_go_mod_path" \
			--arg module_go_mod_sha256 "$module_go_mod_sha256" \
			--arg head "$(jq -r '.head' <<<"$entry")" \
			--arg tree "$(jq -r '.tree' <<<"$entry")" \
			--arg module_tree "$(jq -r '.module_tree' <<<"$entry")" \
			--arg status_path "$status_path" \
			--arg status_sha256 "$status_sha256" \
			--argjson clean "$(jq -r '.clean' <<<"$entry")" \
			--argjson status_bytes "$status_bytes" \
			'.module == $module and .path == $path and .repo_root == $repo_root and .relative_path == $relative_path and .module_go_mod_path == $module_go_mod_path and .module_go_mod_sha256 == $module_go_mod_sha256 and .head == $head and .tree == $tree and .module_tree == $module_tree and .clean == $clean and .status_path == $status_path and .status_bytes == $status_bytes and .status_sha256 == $status_sha256' \
			"$evidence_path" >/dev/null; then
			fail "repository evidence does not match its manifest record: $evidence_path"
		fi
	done < <(jq -c '.repositories[]' "$manifest")
}

verify() {
	local pre=
	local post=
	local output=
	while (($#)); do
		case $1 in
			--pre)
				(($# >= 2)) || { usage; exit 2; }
				pre=$2
				shift 2
				;;
			--post)
				(($# >= 2)) || { usage; exit 2; }
				post=$2
				shift 2
				;;
			--output)
				(($# >= 2)) || { usage; exit 2; }
				output=$2
				shift 2
				;;
			*)
				usage
				exit 2
				;;
		esac
	done
	[[ -n "$pre" && -n "$post" && -n "$output" ]] || { usage; exit 2; }
	validate_manifest "$pre"
	validate_manifest "$post"
	verify_evidence_files "$pre"
	verify_evidence_files "$post"

	local pre_identity post_identity pre_state post_state
	pre_identity=$(jq -S -c '[.repositories[] | {module, path, repo_root, relative_path, module_go_mod_path}] | sort_by(.module, .path)' "$pre")
	post_identity=$(jq -S -c '[.repositories[] | {module, path, repo_root, relative_path, module_go_mod_path}] | sort_by(.module, .path)' "$post")
	[[ "$pre_identity" == "$post_identity" ]] || fail 'repository identity changed between pre and post capture'
	pre_state=$(jq -S -c '[.repositories[] | {module, module_go_mod_sha256, head, tree, module_tree, clean, status_bytes, status_sha256}] | sort_by(.module)' "$pre")
	post_state=$(jq -S -c '[.repositories[] | {module, module_go_mod_sha256, head, tree, module_tree, clean, status_bytes, status_sha256}] | sort_by(.module)' "$post")
	[[ "$pre_state" == "$post_state" ]] || fail 'repository content or cleanliness drifted between pre and post capture'

	if ! jq -e 'all(.repositories[]; .clean == true and .status_bytes == 0)' "$pre" >/dev/null ||
		! jq -e 'all(.repositories[]; .clean == true and .status_bytes == 0)' "$post" >/dev/null; then
		fail 'repository provenance requires clean pre and post repositories'
	fi
	[[ "$(jq -S -c '{module_root, go_mod_path, go_mod_sha256, max_status_bytes}' "$pre")" == "$(jq -S -c '{module_root, go_mod_path, go_mod_sha256, max_status_bytes}' "$post")" ]] ||
		fail 'module provenance identity changed between pre and post capture'

	absolute_path "$output" || fail "output file must be an absolute, single-line path: $output"
	ensure_new_file "$output"
	local output_dir
	output_dir=$(dirname -- "$output")
	[[ -d "$output_dir" && ! -L "$output_dir" ]] || fail "output parent directory is unavailable or unsafe: $output_dir"

	jq -S -n \
		--slurpfile before "$pre" \
		--slurpfile after "$post" \
		'{schema_version: 1, verified: true, module_root: $before[0].module_root, go_mod_path: $before[0].go_mod_path, go_mod_sha256: $before[0].go_mod_sha256, max_status_bytes: $before[0].max_status_bytes, repositories: [
			$before[0].repositories[] as $pre |
			($after[0].repositories[] | select(.module == $pre.module)) as $post |
			{module: $pre.module, path: $pre.path, repo_root: $pre.repo_root, relative_path: $pre.relative_path, module_go_mod_path: $pre.module_go_mod_path, module_go_mod_sha256: $pre.module_go_mod_sha256,
			 pre: {head: $pre.head, tree: $pre.tree, module_tree: $pre.module_tree, clean: $pre.clean, status_path: $pre.status_path, status_bytes: $pre.status_bytes, status_sha256: $pre.status_sha256, evidence_path: $pre.evidence_path, evidence_sha256: $pre.evidence_sha256},
			 post: {head: $post.head, tree: $post.tree, module_tree: $post.module_tree, clean: $post.clean, status_path: $post.status_path, status_bytes: $post.status_bytes, status_sha256: $post.status_sha256, evidence_path: $post.evidence_path, evidence_sha256: $post.evidence_sha256}}
		]}' \
		>"$output"
	chmod 600 "$output"
	printf 'repository-provenance: verified pre/post repository evidence at %s\n' "$output"
}

main() {
	(($# >= 1)) || { usage; exit 2; }
	local command=$1
	shift
	case "$command" in
		capture) capture "$@" ;;
		verify) verify "$@" ;;
		*) usage; exit 2 ;;
	esac
}

main "$@"
