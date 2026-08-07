#![forbid(unsafe_code)]

//! Loopward's deterministic conformance testkit
//! (`docs/10-mvp-and-evaluation.md` §5, §9.1): a scripted fake runner, a
//! failure injector that seeds Phase One's eight named crash points into a
//! real durable store, and an in-memory trace recorder — the shared,
//! reusable versions of the fakes `loopward-runtime`'s own test suite
//! (`advance_episode.rs`, `recovery_matrix.rs`, `port_contract.rs`) each
//! independently defined before this crate existed.
//!
//! This crate depends on every Phase One public API and adapter trait it
//! doubles for (`loopward-api`, `loopward-core`, `loopward-runtime`,
//! `loopward-observability`) but never on a concrete production adapter —
//! a conformance test using it still supplies its own real
//! `loopward-store-sqlite` store, exactly as
//! `crates/loopward-cli/tests/e2e_phase_one.rs` does, so what gets
//! exercised end to end is the real durability path, not a second,
//! parallel in-memory one.

pub mod clock;
pub mod failure;
pub mod fake_runner;
pub mod trace_recorder;

pub use clock::{FixedClock, SequentialIdGenerator};
pub use failure::{CrashFixture, CrashPoint, FailureInjector};
pub use fake_runner::{AttemptScript, FakeRunner};
pub use trace_recorder::TraceRecorder;
