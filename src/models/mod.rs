//! Data models for StreamForge
//!
//! This module contains all the domain models used throughout the application,
//! including event structures, validation logic, and data transformations.

pub mod error;
pub mod event;
pub mod validation;

// Re-export commonly used types
pub use error::{ValidationError, ValidationErrorKind};
pub use event::{EventType, NormalizedEvent, RawEvent};
pub use validation::{validate_event_type, validate_rfc3339, validate_uuid};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn test_module_exports() {
        // Ensure all key types are accessible
        let _raw = RawEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "CLICK".to_string(),
            occurred_at: Utc::now().to_rfc3339(),
            user_id: Uuid::new_v4().to_string(),
            amount_cents: Some(100),
            path: Some("/home".to_string()),
            referrer: None,
        };

        let _event_type = EventType::Click;
        let _error = ValidationError::new(ValidationErrorKind::InvalidUuid, "test");
    }
}
