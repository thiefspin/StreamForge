//! StreamForge Library
//!
//! This library exposes the core modules of StreamForge for use in integration tests
//! and as a library for other applications.

pub mod api;
pub mod config;
pub mod error;
pub mod logging;

// Re-export commonly used types at the crate root
pub use config::Config;
pub use error::{Error, Result};

// Re-export API server functions
pub use api::server::{create_router, create_server, shutdown_signal};

// Re-export health check types
pub use api::{
    BuildInfo, ComponentHealth, HealthResponse, HealthState, HealthStatus, ReadyResponse,
};
