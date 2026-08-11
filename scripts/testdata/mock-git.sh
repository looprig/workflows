#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${1:-} != -C || $# -lt 3 ]]; then
	printf 'mock git: expected -C <path> <command>\n' >&2
	exit 2
fi
path=$2
shift 2

workflow_root=${MOCK_WORKFLOWS_ROOT:?}
replacement_root=${MOCK_REPLACEMENT_ROOT:?}
mode=${MOCK_GIT_MODE:-clean}
phase=${MOCK_GIT_PHASE:-pre}

if [[ "$path" == "$workflow_root" ]]; then
	identity=workflow
elif [[ "$path" == "$replacement_root" ]]; then
	identity=replacement
else
	printf 'mock git: unexpected repository path: %s\n' "$path" >&2
	exit 2
fi

if [[ "$identity" == workflow && "$mode" == unavailable-source ]] ||
	[[ "$identity" == replacement && "$mode" == unavailable-replacement ]]; then
	printf 'mock git: repository metadata unavailable\n' >&2
	exit 1
fi

hex_for() {
	local digit=$1
	printf '%*s' 40 '' | tr ' ' "$digit"
}

case ${1:-} in
	rev-parse)
		case ${2:-} in
		--show-toplevel)
			printf '%s\n' "$path"
			;;
		--show-prefix)
			printf '\n'
			;;
		--verify)
			case ${3:-} in
			HEAD^{commit})
				if [[ "$identity" == workflow && "$mode" == drift-source && "$phase" == post ]]; then
					hex_for 3
				elif [[ "$identity" == replacement && "$mode" == drift-replacement && "$phase" == post ]]; then
					hex_for 4
				elif [[ "$identity" == workflow ]]; then
					hex_for 1
				else
					hex_for 2
				fi
				;;
			HEAD^{tree})
				if [[ "$identity" == workflow && "$mode" == drift-source && "$phase" == post ]]; then
					hex_for 5
				elif [[ "$identity" == replacement && "$mode" == drift-replacement && "$phase" == post ]]; then
					hex_for 6
				elif [[ "$identity" == workflow ]]; then
					hex_for 7
				else
					hex_for 8
				fi
				;;
			HEAD:*)
				if [[ "$identity" == workflow ]]; then
					hex_for 9
				else
					hex_for a
				fi
				;;
			*)
				printf 'mock git: unsupported rev-parse verification: %s\n' "${3:-}" >&2
				exit 2
				;;
			esac
			;;
		*)
			printf 'mock git: unsupported rev-parse operation: %s\n' "${2:-}" >&2
			exit 2
			;;
		esac
		;;
	status)
		if [[ "$identity" == workflow && "$mode" == dirty-source ]]; then
			printf ' M source.go\n'
		elif [[ "$identity" == replacement && "$mode" == dirty-replacement ]]; then
			printf '?? replacement.go\n'
		fi
		;;
	*)
		printf 'mock git: unsupported command: %s\n' "${1:-}" >&2
		exit 2
		;;
esac
