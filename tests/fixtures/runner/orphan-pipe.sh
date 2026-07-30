#!/bin/sh
# A Work Unit runner command that writes a valid result and exits promptly,
# but leaves a backgrounded grandchild holding the inherited stdout/stderr
# descriptors well past its own exit.
#
# A pipe reaches end-of-file only when every holder of its write end closes
# it, so a runner that waits unconditionally for its capture tasks would stall
# on this orphan for the orphan's whole lifetime, even though the command it
# actually spawned is definitively reaped. Collecting this attempt must not
# hang, and cancelling it must not be misreported.

set -eu

# Inherits stdout and stderr, and outlives this script by a wide margin.
sleep 10 &

printf 'orphan-pipe runner started\n'
printf 'orphan-pipe runner backgrounded a pipe holder\n' >&2

cat >"$LOOPWARD_RESULT_PATH" <<EOF
{
  "protocol_version": "loopward.api/v0.1",
  "attempt_id": "${LOOPWARD_ATTEMPT_ID}",
  "outcome": "produced",
  "recommended_continuation": null
}
EOF
