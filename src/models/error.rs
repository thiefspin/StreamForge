//! Validation error types for StreamForge models
//!
//! This module defines error types specifically for data validation,
//! separate from the general application errors.

use std::fmt;
use thiserror::Error;

/// Main validation error type
#[derive(Error, Debug, Clone)]
pub struct ValidationError {
    /// The kind of validation error
    pub kind: ValidationErrorKind,
    /// The field that failed validation
    pub field: String,
    /// Optional additional context
    pub context: Option<String>,
}

impl ValidationError {
    /// Create a new validation error
    pub fn new(kind: ValidationErrorKind, field: impl Into<String>) -> Self {
        Self {
            kind,
            field: field.into(),
            context: None,
        }
    }

    /// Create a validation error with additional context
    pub fn with_context(
        kind: ValidationErrorKind,
        field: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            field: field.into(),
            context: Some(context.into()),
        }
    }

    /// Add context to an existing error
    pub fn add_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.context {
            Some(ctx) => write!(
                f,
                "Validation failed for field '{}': {} - {}",
                self.field, self.kind, ctx
            ),
            None => write!(
                f,
                "Validation failed for field '{}': {}",
                self.field, self.kind
            ),
        }
    }
}

/// Specific validation error types
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ValidationErrorKind {
    /// Invalid UUID format
    #[error("Invalid UUID format")]
    InvalidUuid,

    /// Invalid event type
    #[error("Invalid event type (expected: CLICK, VIEW, or PURCHASE)")]
    InvalidEventType,

    /// Invalid timestamp format
    #[error("Invalid RFC3339 timestamp format")]
    InvalidTimestamp,

    /// Invalid amount (negative value)
    #[error("Amount must be non-negative")]
    NegativeAmount,

    /// Field is required but missing
    #[error("Required field is missing")]
    RequiredField,

    /// Field value is too long
    #[error("Value exceeds maximum length")]
    TooLong { max: usize },

    /// Field value is too short
    #[error("Value is below minimum length")]
    TooShort { min: usize },

    /// Invalid URL format
    #[error("Invalid URL format")]
    InvalidUrl,

    /// Custom validation error
    #[error("{0}")]
    Custom(String),
}

/// Result type alias for validation operations
pub type ValidationResult<T> = Result<T, ValidationError>;

/// Collection of validation errors
#[derive(Debug, Default, Clone)]
pub struct ValidationErrors {
    errors: Vec<ValidationError>,
}

impl ValidationErrors {
    /// Create a new empty collection
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a validation error to the collection
    pub fn add(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    /// Check if there are any errors
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get the number of errors
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// Get all errors
    pub fn errors(&self) -> &[ValidationError] {
        &self.errors
    }

    /// Convert to a Result
    pub fn into_result<T>(self, value: T) -> Result<T, Self> {
        if self.is_empty() {
            Ok(value)
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            write!(f, "No validation errors")
        } else {
            write!(f, "Validation failed with {} error(s):", self.errors.len())?;
            for error in &self.errors {
                write!(f, "\n  - {}", error)?;
            }
            Ok(())
        }
    }
}

impl std::error::Error for ValidationErrors {}

impl From<ValidationError> for ValidationErrors {
    fn from(error: ValidationError) -> Self {
        let mut errors = Self::new();
        errors.add(error);
        errors
    }
}

/// Convert validation errors to application errors
impl From<ValidationError> for crate::error::Error {
    fn from(err: ValidationError) -> Self {
        crate::error::Error::validation(err.to_string())
    }
}

impl From<ValidationErrors> for crate::error::Error {
    fn from(err: ValidationErrors) -> Self {
        crate::error::Error::validation(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_creation() {
        let error = ValidationError::new(ValidationErrorKind::InvalidUuid, "event_id");
        assert_eq!(error.field, "event_id");
        assert!(error.context.is_none());
    }

    #[test]
    fn test_validation_error_with_context() {
        let error = ValidationError::with_context(
            ValidationErrorKind::InvalidUuid,
            "event_id",
            "Expected UUID v4 format",
        );
        assert_eq!(error.field, "event_id");
        assert_eq!(error.context.as_deref(), Some("Expected UUID v4 format"));
    }

    #[test]
    fn test_validation_error_display() {
        let error = ValidationError::new(ValidationErrorKind::InvalidUuid, "event_id");
        let display = error.to_string();
        assert!(display.contains("event_id"));
        assert!(display.contains("Invalid UUID"));
    }

    #[test]
    fn test_validation_errors_collection() {
        let mut errors = ValidationErrors::new();
        assert!(errors.is_empty());

        errors.add(ValidationError::new(
            ValidationErrorKind::InvalidUuid,
            "event_id",
        ));
        errors.add(ValidationError::new(
            ValidationErrorKind::InvalidEventType,
            "event_type",
        ));

        assert_eq!(errors.len(), 2);
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_validation_errors_into_result() {
        let mut errors = ValidationErrors::new();
        let result = errors.clone().into_result("success");
        assert!(result.is_ok());

        errors.add(ValidationError::new(
            ValidationErrorKind::InvalidUuid,
            "test",
        ));
        let result = errors.into_result("fail");
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_error_kinds() {
        let kinds = vec![
            ValidationErrorKind::InvalidUuid,
            ValidationErrorKind::InvalidEventType,
            ValidationErrorKind::InvalidTimestamp,
            ValidationErrorKind::NegativeAmount,
            ValidationErrorKind::RequiredField,
            ValidationErrorKind::TooLong { max: 100 },
            ValidationErrorKind::TooShort { min: 1 },
            ValidationErrorKind::InvalidUrl,
            ValidationErrorKind::Custom("test".to_string()),
        ];

        for kind in kinds {
            let error = ValidationError::new(kind.clone(), "test_field");
            assert!(!error.to_string().is_empty());
        }
    }
}
