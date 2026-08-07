#!/bin/sh
# A long-running Work Unit runner command used to exercise cancellation.
#
# Modes (first argument):
#   cooperative - the default: honour SIGTERM and exit promptly (128 + 15)
#   ignore-term - ignore SIGTERM entirely, forcing an escalation to SIGKILL
#
# The loop sleeps in short slices so a trap fires within a few tens of
# milliseconds, keeping the cancellation tests fast and deterministic.

set -eu

mode="${1:-cooperative}"

if [ "$mode" = "ignore-term" ]; then
	trap '' TERM
else
	trap 'exit 143' TERM
fi

printf 'slow runner started\n'

i=0
while [ "$i" -lt 600 ]; do
	sleep 0.05
	i=$((i + 1))
done

printf 'slow runner finished without being cancelled\n'
