#[test]
fn workspace_uses_phase_one_protocol_version() {
    assert_eq!(loopward_api::PROTOCOL_VERSION, "loopward.api/v0.1");
}
