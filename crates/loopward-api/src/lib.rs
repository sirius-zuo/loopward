#![forbid(unsafe_code)]

//! Loopward's versioned public wire contract.
//!
//! This crate defines durable identifiers, context lifecycle DTOs, the Work
//! Unit and runner protocol, tracing carrier types, and the inbound
//! `LoopService` API described in `docs/02` through `docs/09`. It is
//! contracts only: no runtime behavior, storage, or verification logic.
//! Every other Loopward crate depends on the shapes defined here.

/// The service protocol version this crate's contracts implement.
pub const PROTOCOL_VERSION: &str = "loopward.api/v0.1";

pub mod context;
pub mod error;
pub mod event;
pub mod ids;
pub mod loop_service;
pub mod resource;
pub mod runner;
pub mod trace;
pub mod work_unit;

pub use error::{LoopwardError, LoopwardErrorCode};
pub use loop_service::LoopService;
pub use resource::ResourceRef;
pub use runner::WorkUnitRunner;
pub use trace::TraceContext;
pub use work_unit::{WorkUnitRequest, WorkUnitResult};
