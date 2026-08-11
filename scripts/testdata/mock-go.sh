#!/usr/bin/env bash
set -Eeuo pipefail

case "$*" in
	"list -m -json all")
		printf '%s\n' '{"Path":"github.com/looprig/workflows","Version":"v0.0.0"}'
		;;
	"list -deps -json ./...")
		printf '%s\n' '{"ImportPath":"github.com/looprig/workflows","Imports":[]}'
		;;
	*)
		exit 1
		;;
esac
