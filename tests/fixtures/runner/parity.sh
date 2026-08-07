#!/bin/sh
# A minimal, deterministic Work Unit runner command used only to demonstrate
# command-runner-profile parity
# (`crates/loopward-cli/tests/runner_profile_parity.rs`): this script and
# `loopward_testkit::FakeRunner` (scripted identically in that test file)
# both complete the same Work Unit protocol fixture with an equivalent
# `produced` outcome and the same declared output resource, satisfying
# `docs/10-mvp-and-evaluation.md` §13's "at least two different
# command-runner profiles complete the same Work Unit protocol fixture"
# acceptance gate.
#
# It reads nothing from the materialized request or Boot Package — the
# fixture's shared expectations live in the Rust test, not in this script —
# and simply proposes a fixed result for whichever attempt it was invoked
# for. The declared output resource below must stay byte-for-byte identical
# to `fixture_output()` in the Rust test.

set -eu

cat >"$LOOPWARD_RESULT_PATH" <<EOF
{
  "protocol_version": "loopward.api/v0.1",
  "attempt_id": "${LOOPWARD_ATTEMPT_ID}",
  "outcome": "produced",
  "outputs": [
    {
      "uri": "loopward-fixture://runner-profile-parity/output",
      "media_type": "text/plain; charset=utf-8",
      "digest": "sha256:runner-profile-parity-fixture",
      "version": null,
      "size": 42,
      "summary": "loopward-runner-profile-parity fixture output"
    }
  ],
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
