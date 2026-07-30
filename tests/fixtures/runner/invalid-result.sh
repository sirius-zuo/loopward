#!/bin/sh
# A Work Unit runner command that exits successfully but writes a truncated,
# unparseable result document. Collecting it must fail structural validation
# with InvalidResult rather than being treated as an accepted outcome.

set -eu

printf 'invalid-result runner starting\n'
printf 'invalid-result runner will write malformed JSON\n' >&2

printf '{ "protocol_version": "loopward.api/v0.1", "attempt_id": ' >"$LOOPWARD_RESULT_PATH"
