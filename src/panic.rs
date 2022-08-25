//! Types for handling panics.

/// Contains information about the panics that workers encountered.
#[derive(Debug, Clone)]
pub struct PanicInfo {
    pub request: String,
}
