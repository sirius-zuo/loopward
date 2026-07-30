//! On-disk materialization of the Work Unit request/result protocol, per
//! `docs/05-work-unit-and-runner-protocol.md` §13-14.
//!
//! Everything a runner command is allowed to see for one Attempt lives in
//! one isolated directory. The command receives file paths and identifiers,
//! never Loop database credentials and never a store handle: this module is
//! the only place the protocol touches the filesystem, and it writes exactly
//! the request, the Boot Package manifest, and the empty input/output
//! directories.
//!
//! ```text
//! <run_root>/<attempt_id>/
//! ├── request.json
//! ├── boot-package.json
//! ├── inputs/
//! ├── outputs/
//! ├── stdout.log
//! ├── stderr.log
//! └── result.json
//! ```

use std::path::{Path, PathBuf};

use loopward_api::ids::AttemptId;
use loopward_api::runner::{RunnerError, RunnerErrorCode};
use loopward_api::work_unit::{WorkUnitRequest, WorkUnitResult};

use crate::runner_error;

/// The file the materialized [`WorkUnitRequest`] is written to.
pub const REQUEST_FILE_NAME: &str = "request.json";
/// The file the materialized Boot Package manifest is written to.
pub const BOOT_PACKAGE_FILE_NAME: &str = "boot-package.json";
/// The directory holding materialized Work Unit inputs.
pub const INPUTS_DIR_NAME: &str = "inputs";
/// The directory the command writes produced resources into.
pub const OUTPUTS_DIR_NAME: &str = "outputs";
/// The file the command's standard output is captured to.
pub const STDOUT_LOG_FILE_NAME: &str = "stdout.log";
/// The file the command's standard error is captured to.
pub const STDERR_LOG_FILE_NAME: &str = "stderr.log";
/// The file the command is expected to write its proposed result to.
pub const RESULT_FILE_NAME: &str = "result.json";

/// The environment variable carrying the owning Loop's identifier.
pub const ENV_LOOP_ID: &str = "LOOPWARD_LOOP_ID";
/// The environment variable carrying the owning Episode's identifier.
pub const ENV_EPISODE_ID: &str = "LOOPWARD_EPISODE_ID";
/// The environment variable carrying the Runtime Session's identifier.
pub const ENV_SESSION_ID: &str = "LOOPWARD_SESSION_ID";
/// The environment variable carrying the Runtime Session's generation.
pub const ENV_SESSION_GENERATION: &str = "LOOPWARD_SESSION_GENERATION";
/// The environment variable carrying the Work Unit's identifier.
pub const ENV_WORK_UNIT_ID: &str = "LOOPWARD_WORK_UNIT_ID";
/// The environment variable carrying the Attempt's identifier.
pub const ENV_ATTEMPT_ID: &str = "LOOPWARD_ATTEMPT_ID";
/// The environment variable carrying the materialized request's path.
pub const ENV_REQUEST_PATH: &str = "LOOPWARD_REQUEST_PATH";
/// The environment variable carrying the expected result path.
pub const ENV_RESULT_PATH: &str = "LOOPWARD_RESULT_PATH";
/// The environment variable carrying the resource output directory.
pub const ENV_RESOURCE_DIR: &str = "LOOPWARD_RESOURCE_DIR";
/// The environment variable carrying the W3C `traceparent`, exported only
/// when the request actually carries one.
pub const ENV_TRACEPARENT: &str = "TRACEPARENT";
/// The environment variable carrying the W3C `tracestate`, exported only
/// when the request actually carries one.
pub const ENV_TRACESTATE: &str = "TRACESTATE";

/// One Attempt's isolated on-disk directory layout.
///
/// Constructing a layout computes paths only; nothing touches the
/// filesystem until [`AttemptLayout::materialize`] runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptLayout {
    root: PathBuf,
}

impl AttemptLayout {
    /// Computes the layout for `attempt_id` beneath `run_root`.
    #[must_use]
    pub fn new(run_root: &Path, attempt_id: AttemptId) -> Self {
        Self {
            root: run_root.join(attempt_id.to_string()),
        }
    }

    /// The Attempt's root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The materialized request path.
    #[must_use]
    pub fn request_path(&self) -> PathBuf {
        self.root.join(REQUEST_FILE_NAME)
    }

    /// The materialized Boot Package manifest path.
    #[must_use]
    pub fn boot_package_path(&self) -> PathBuf {
        self.root.join(BOOT_PACKAGE_FILE_NAME)
    }

    /// The materialized inputs directory.
    #[must_use]
    pub fn inputs_dir(&self) -> PathBuf {
        self.root.join(INPUTS_DIR_NAME)
    }

    /// The resource output directory the command writes into.
    #[must_use]
    pub fn outputs_dir(&self) -> PathBuf {
        self.root.join(OUTPUTS_DIR_NAME)
    }

    /// The captured standard-output log path.
    #[must_use]
    pub fn stdout_log_path(&self) -> PathBuf {
        self.root.join(STDOUT_LOG_FILE_NAME)
    }

    /// The captured standard-error log path.
    #[must_use]
    pub fn stderr_log_path(&self) -> PathBuf {
        self.root.join(STDERR_LOG_FILE_NAME)
    }

    /// The path the command is expected to write its result to.
    #[must_use]
    pub fn result_path(&self) -> PathBuf {
        self.root.join(RESULT_FILE_NAME)
    }

    /// Creates the directory tree and writes `request.json` and
    /// `boot-package.json`.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerErrorCode::ResourceError`] when the attempt directory
    /// cannot be created or the protocol documents cannot be written.
    pub async fn materialize(&self, request: &WorkUnitRequest) -> Result<(), RunnerError> {
        for directory in [self.root.clone(), self.inputs_dir(), self.outputs_dir()] {
            tokio::fs::create_dir_all(&directory).await.map_err(|err| {
                runner_error(
                    RunnerErrorCode::ResourceError,
                    format!(
                        "could not create attempt directory `{}`: {err}",
                        directory.display()
                    ),
                    true,
                )
            })?;
        }

        write_json(&self.request_path(), request).await?;
        write_json(&self.boot_package_path(), &request.boot_package).await?;
        Ok(())
    }
}

async fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), RunnerError> {
    let encoded = serde_json::to_vec_pretty(value).map_err(|err| {
        runner_error(
            RunnerErrorCode::ProtocolError,
            format!("could not encode `{}`: {err}", path.display()),
            false,
        )
    })?;
    tokio::fs::write(path, encoded).await.map_err(|err| {
        runner_error(
            RunnerErrorCode::ResourceError,
            format!("could not write `{}`: {err}", path.display()),
            true,
        )
    })
}

/// A parsed Loopward protocol version of the form `<namespace>/v<major>.<minor>`.
///
/// Only the namespace and the major component participate in compatibility:
/// per `docs/05-work-unit-and-runner-protocol.md` §14 an incompatible major
/// version is rejected, while a differing minor version is additive and
/// therefore acceptable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolVersion {
    namespace: String,
    major: u32,
}

impl ProtocolVersion {
    /// Parses a version string, returning `None` when it does not follow the
    /// `<namespace>/v<major>[.<minor>...]` shape.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let (namespace, version) = raw.rsplit_once('/')?;
        if namespace.is_empty() {
            return None;
        }
        let major = version
            .strip_prefix('v')?
            .split('.')
            .next()?
            .parse::<u32>()
            .ok()?;
        Some(Self {
            namespace: namespace.to_owned(),
            major,
        })
    }

    /// Whether two versions share a namespace and a major component.
    #[must_use]
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.major == other.major
    }
}

/// Rejects `version` unless it shares a namespace and major component with
/// one of `accepted`.
///
/// # Errors
///
/// Returns [`RunnerErrorCode::ProtocolError`] when `version` is unparseable
/// or when no accepted version shares its major component.
pub fn ensure_compatible_version(
    context: &str,
    version: &str,
    accepted: &[String],
) -> Result<(), RunnerError> {
    let parsed = ProtocolVersion::parse(version).ok_or_else(|| {
        runner_error(
            RunnerErrorCode::ProtocolError,
            format!("{context} carries an unparseable protocol version `{version}`"),
            false,
        )
    })?;

    let compatible = accepted
        .iter()
        .filter_map(|candidate| ProtocolVersion::parse(candidate))
        .any(|candidate| parsed.is_compatible_with(&candidate));

    if compatible {
        Ok(())
    } else {
        Err(runner_error(
            RunnerErrorCode::ProtocolError,
            format!(
                "{context} carries protocol version `{version}`, which is incompatible with the accepted versions [{}]",
                accepted.join(", ")
            ),
            false,
        ))
    }
}

/// Reads, size-checks, version-checks, and deserializes the command's
/// proposed result.
///
/// The returned [`WorkUnitResult`] is a proposal boundary only: this
/// function performs structural validation, never acceptance.
///
/// # Errors
///
/// Returns [`RunnerErrorCode::InvalidResult`] when the file is missing, is
/// not a regular file, exceeds `max_bytes`, is not valid JSON, omits
/// `protocol_version`, does not deserialize into a [`WorkUnitResult`], or
/// names a different Attempt. Returns [`RunnerErrorCode::ProtocolError`]
/// when the declared protocol major version is incompatible.
pub async fn read_result(
    path: &Path,
    attempt_id: AttemptId,
    max_bytes: u64,
    accepted_versions: &[String],
) -> Result<WorkUnitResult, RunnerError> {
    let invalid = |message: String| runner_error(RunnerErrorCode::InvalidResult, message, false);

    let metadata = tokio::fs::metadata(path).await.map_err(|err| {
        invalid(format!(
            "the command did not write a result to `{}`: {err}",
            path.display()
        ))
    })?;

    if !metadata.is_file() {
        return Err(invalid(format!(
            "the result path `{}` is not a regular file",
            path.display()
        )));
    }

    if metadata.len() > max_bytes {
        return Err(invalid(format!(
            "the result file is {} bytes, exceeding the configured maximum of {max_bytes} bytes",
            metadata.len()
        )));
    }

    let bytes = tokio::fs::read(path)
        .await
        .map_err(|err| invalid(format!("could not read `{}`: {err}", path.display())))?;

    let document: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|err| invalid(format!("the result is not valid JSON: {err}")))?;

    let declared = document
        .get("protocol_version")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("the result omits `protocol_version`".to_owned()))?;
    ensure_compatible_version("the result", declared, accepted_versions)?;

    let result: WorkUnitResult = serde_json::from_value(document).map_err(|err| {
        invalid(format!(
            "the result does not satisfy the WorkUnitResult contract: {err}"
        ))
    })?;

    if result.attempt_id != attempt_id {
        return Err(invalid(format!(
            "the result names attempt `{}` but was collected for attempt `{attempt_id}`",
            result.attempt_id
        )));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{ProtocolVersion, ensure_compatible_version};

    #[test]
    fn parses_a_namespaced_versioned_protocol_identifier() {
        let parsed = ProtocolVersion::parse("loopward.api/v0.1").expect("parses");
        assert!(parsed.is_compatible_with(&ProtocolVersion::parse("loopward.api/v0.9").unwrap()));
        assert!(!parsed.is_compatible_with(&ProtocolVersion::parse("loopward.api/v1.0").unwrap()));
        assert!(!parsed.is_compatible_with(&ProtocolVersion::parse("other.api/v0.1").unwrap()));
    }

    #[test]
    fn rejects_unparseable_versions() {
        assert!(ProtocolVersion::parse("loopward.api").is_none());
        assert!(ProtocolVersion::parse("loopward.api/0.1").is_none());
        assert!(ProtocolVersion::parse("/v0.1").is_none());
        assert!(ProtocolVersion::parse("loopward.api/vx").is_none());
    }

    #[test]
    fn a_matching_major_is_accepted_and_a_differing_major_is_not() {
        let accepted = vec!["loopward.api/v0.1".to_owned()];
        assert!(ensure_compatible_version("the request", "loopward.api/v0.4", &accepted).is_ok());
        assert!(ensure_compatible_version("the request", "loopward.api/v1.0", &accepted).is_err());
    }
}
