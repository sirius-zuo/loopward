#!/bin/sh
# A reference Work Unit runner command that succeeds.
#
# It demonstrates the generic command runner protocol documented in
# docs/05-work-unit-and-runner-protocol.md section 13: read the request from
# LOOPWARD_REQUEST_PATH, write produced resources into LOOPWARD_RESOURCE_DIR,
# and write the proposed result to LOOPWARD_RESULT_PATH.
#
# Modes (first argument):
#   normal        - the default: echo the environment and write a valid result
#   flood         - additionally emit far more stdout/stderr than any sane
#                   capture limit, to exercise bounded capture
#   wrong-version - write a result carrying an incompatible protocol major

set -eu

mode="${1:-normal}"

# Echo every documented variable so a test can assert exactly which ones were
# exported. Unset variables are reported as `<unset>` rather than as empty, so
# "absent" and "present but empty" stay distinguishable.
for var in \
	LOOPWARD_LOOP_ID \
	LOOPWARD_EPISODE_ID \
	LOOPWARD_SESSION_ID \
	LOOPWARD_SESSION_GENERATION \
	LOOPWARD_WORK_UNIT_ID \
	LOOPWARD_ATTEMPT_ID \
	LOOPWARD_REQUEST_PATH \
	LOOPWARD_RESULT_PATH \
	LOOPWARD_RESOURCE_DIR \
	TRACEPARENT \
	TRACESTATE; do
	if value=$(printenv "$var"); then
		printf '%s=%s\n' "$var" "$value"
	else
		printf '%s=<unset>\n' "$var"
	fi
done

printf 'runner stderr marker\n' >&2

if [ "$mode" = "flood" ]; then
	i=0
	while [ "$i" -lt 256 ]; do
		printf 'stdout-flood-%s-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' "$i"
		printf 'stderr-flood-%s-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n' "$i" >&2
		i=$((i + 1))
	done
fi

protocol_version="loopward.api/v0.1"
if [ "$mode" = "wrong-version" ]; then
	protocol_version="loopward.api/v9.0"
fi

cat >"$LOOPWARD_RESULT_PATH" <<EOF
{
  "protocol_version": "${protocol_version}",
  "attempt_id": "${LOOPWARD_ATTEMPT_ID}",
  "outcome": "produced",
  "outputs": [],
  "state_proposals": [],
  "observation_proposals": [],
  "decision_proposals": [],
  "verification_evidence": [],
  "unresolved": [],
  "recommended_continuation": null,
  "usage": { "known": {}, "estimated": {}, "unavailable": [] },
  "runner_trace_refs": []
}
EOF
