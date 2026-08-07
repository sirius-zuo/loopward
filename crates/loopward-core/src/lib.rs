#![forbid(unsafe_code)]

//! Loopward's pure domain core: the durable state machines that decide what
//! a Loop may do next.
//!
//! This crate is the reducer at the heart of the system. It is synchronous,
//! deterministic, and I/O-free: no Tokio, no async, no storage, no clock, no
//! network, no filesystem. Given a state, a command, and a resolved policy,
//! [`decide`] returns the durable events that would follow or a
//! [`DomainError`]; [`evolve`] applies one event; [`validate_invariants`]
//! rejects any state that violates the model. Everything that talks to the
//! outside world lives in `loopward-runtime` and drives these three
//! functions.
//!
//! # What it deliberately does not contain
//!
//! No orchestration, DAG, workflow topology, agent registry, or Aether
//! concept. No domain entities either: Git revisions, claims, sources,
//! hypotheses, and experiments are the caller's business, carried as
//! schema-declared domain state (`docs/02-core-domain-model.md` §11).
//!
//! # The three rules the model exists to enforce
//!
//! * **A Loop is durable; a Runtime Session is not.** Sessions are bounded,
//!   generation-numbered, and replaceable, and closing one never terminates
//!   the Loop ([`RuntimeSession`], [`LoopExecution`]).
//! * **A runner result is a proposal.** [`LifecycleCommand::RecordResult`]
//!   moves a Work Unit to `AwaitingVerification` and no further; only
//!   [`LifecycleCommand::RecordVerification`] with
//!   [`VerificationOutcome::Accepted`] can accept it, and
//!   [`validate_invariants`] rejects any state that claims otherwise.
//! * **Every mutation is fenced.** [`LoopExecution::aggregate_revision`]
//!   advances on every event and
//!   [`LoopExecution::checkpoint_version`] only on Checkpoint commit, so the
//!   runtime always has the expected revision to fence a write against.
//!
//! # Types shared with `loopward-api`
//!
//! [`WorkUnit`], [`WorkUnitStatus`], and [`LoopKind`] are re-exported from
//! `loopward-api` rather than redefined. Their `docs/02-core-domain-model.md`
//! shapes are already part of the wire contract (`WorkUnitRequest` carries a
//! whole `WorkUnit`), and a byte-identical second definition here would only
//! add a conversion layer for `loopward-runtime` to get wrong.
//!
//! # Example
//!
//! ```
//! use loopward_core::{EffectivePolicy, LifecycleCommand, decide, evolve, validate_invariants};
//! # use loopward_api::ids::{CheckpointId, EpisodeId, LoopId};
//! # use loopward_api::loop_service::LoopKind;
//! # use loopward_api::resource::PhaseRef;
//! # use loopward_api::work_unit::ResourceBudget;
//! # use loopward_core::{AggregateState, Checkpoint, LoopExecution, LoopStatus};
//! # let loop_id = LoopId::new();
//! # let state = AggregateState {
//! #     execution: LoopExecution {
//! #         id: loop_id,
//! #         kind: LoopKind::new("domain.example"),
//! #         status: LoopStatus::Active,
//! #         phase: PhaseRef::new("implementation"),
//! #         aggregate_revision: 1,
//! #         checkpoint_version: 1,
//! #         active_episode: None,
//! #         active_work_unit: None,
//! #         budget: ResourceBudget::default(),
//! #     },
//! #     episode: None,
//! #     session: None,
//! #     work_unit: None,
//! #     attempt: None,
//! #     checkpoint: Checkpoint {
//! #         id: CheckpointId::new(),
//! #         loop_id,
//! #         version: 1,
//! #         aggregate_revision: 1,
//! #         parent: None,
//! #         episode_id: None,
//! #         integrity_digest: None,
//! #     },
//! # };
//! let command = LifecycleCommand::StartEpisode { episode_id: EpisodeId::new() };
//! let events = decide(&state, &command, &EffectivePolicy::default())?;
//!
//! let mut state = state;
//! for event in &events {
//!     state = evolve(state, event)?;
//!     validate_invariants(&state)?;
//! }
//! # Ok::<(), loopward_core::DomainError>(())
//! ```

pub mod command;
pub mod event;
pub mod invariants;
pub mod model;
pub mod policy;
pub mod reducer;

pub use command::LifecycleCommand;
pub use event::{LoopEvent, RotationDisposition, SessionCloseOutcome};
pub use invariants::{DomainError, Entity, validate_invariants};
pub use model::{
    AggregateState, AttemptStatus, Checkpoint, ContextBudget, Episode, EpisodeKind, EpisodeStatus,
    LoopExecution, LoopKind, LoopStatus, RotationReason, RuntimeSession, SessionStatus,
    SideEffectClass, VerificationOutcome, WorkUnitAttempt, work_unit_status_is_terminal,
    work_unit_status_label,
};
pub use policy::{EffectivePolicy, SessionReusePolicy};
pub use reducer::{decide, evolve};

pub use loopward_api::work_unit::{WorkUnit, WorkUnitStatus};
