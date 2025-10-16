//! Custom validation functions for StreamForge models
//!
//! This module provides reusable validation functions for common field types
//! like UUIDs, timestamps, and domain-specific values.

use chrono::DateTime;
use regex::Regex;
use std::sync::OnceLock;
use uuid::Uuid;
use validator::ValidationError;

use super::error::{ValidationError as ModelValidationError, ValidationErrorKind};

// Lazy static regex patterns
static UUID_REGEX: OnceLock<Regex> = OnceLock::new();
static URL_REGEX: OnceLock<Regex> = OnceLock::new();

/// Get or initialize the UUID regex pattern
fn uuid_regex() -> &'static Regex {
    UUID_REGEX.get_or_init(|| {
        Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
            .expect("Invalid UUID regex pattern")
    })
}

/// Get or initialize the URL regex pattern
fn url_regex() -> &'static Regex {
    URL_REGEX.get_or_init(|| {
        Regex::new(r"^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}(/.*)?$")
            .expect("Invalid URL regex pattern")
    })
}

/// Validate UUID format for validator crate
pub fn validate_uuid(uuid_str: &str) -> Result<(), ValidationError> {
    if Uuid::parse_str(uuid_str).is_ok() {
        Ok(())
    } else {
        Err(ValidationError::new("Invalid UUID format"))
    }
}

/// Validate UUID format returning our custom error type
pub fn validate_uuid_field(uuid_str: &str, field_name: &str) -> Result<Uuid, ModelValidationError> {
    Uuid::parse_str(uuid_str).map_err(|e| {
        ModelValidationError::with_context(
            ValidationErrorKind::InvalidUuid,
            field_name,
            format!("Failed to parse UUID: {}", e),
        )
    })
}

/// Validate UUID using regex (alternative method)
pub fn validate_uuid_regex(uuid_str: &str) -> bool {
    uuid_regex().is_match(uuid_str)
}

/// Validate event type for validator crate
pub fn validate_event_type(event_type: &str) -> Result<(), ValidationError> {
    match event_type.to_uppercase().as_str() {
        "CLICK" | "VIEW" | "PURCHASE" => Ok(()),
        _ => Err(ValidationError::new(
            "Invalid event type. Expected: CLICK, VIEW, or PURCHASE",
        )),
    }
}

/// Validate RFC3339 timestamp for validator crate
pub fn validate_rfc3339(timestamp: &str) -> Result<(), ValidationError> {
    if DateTime::parse_from_rfc3339(timestamp).is_ok() {
        Ok(())
    } else {
        Err(ValidationError::new("Invalid RFC3339 timestamp format"))
    }
}

/// Validate RFC3339 timestamp returning parsed DateTime
pub fn validate_timestamp_field(
    timestamp: &str,
    field_name: &str,
) -> Result<DateTime<chrono::Utc>, ModelValidationError> {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| {
            ModelValidationError::with_context(
                ValidationErrorKind::InvalidTimestamp,
                field_name,
                format!("Failed to parse RFC3339 timestamp: {}", e),
            )
        })
}

/// Validate that an optional amount is non-negative
pub fn validate_amount(
    amount: Option<i64>,
    field_name: &str,
) -> Result<Option<i64>, ModelValidationError> {
    match amount {
        Some(value) if value < 0 => Err(ModelValidationError::with_context(
            ValidationErrorKind::NegativeAmount,
            field_name,
            format!("Amount must be non-negative, got: {}", value),
        )),
        _ => Ok(amount),
    }
}

/// Validate string length constraints
pub fn validate_string_length(
    value: &str,
    field_name: &str,
    min: Option<usize>,
    max: Option<usize>,
) -> Result<(), ModelValidationError> {
    let len = value.len();

    if let Some(min_len) = min {
        if len < min_len {
            return Err(ModelValidationError::with_context(
                ValidationErrorKind::TooShort { min: min_len },
                field_name,
                format!("Value length {} is less than minimum {}", len, min_len),
            ));
        }
    }

    if let Some(max_len) = max {
        if len > max_len {
            return Err(ModelValidationError::with_context(
                ValidationErrorKind::TooLong { max: max_len },
                field_name,
                format!("Value length {} exceeds maximum {}", len, max_len),
            ));
        }
    }

    Ok(())
}

/// Validate an optional URL
pub fn validate_optional_url(
    url: Option<&str>,
    field_name: &str,
) -> Result<(), ModelValidationError> {
    if let Some(url_str) = url {
        if !url_str.is_empty() && !url_regex().is_match(url_str) {
            return Err(ModelValidationError::with_context(
                ValidationErrorKind::InvalidUrl,
                field_name,
                format!("Invalid URL format: {}", url_str),
            ));
        }
    }
    Ok(())
}

/// Validate a required field is not empty
pub fn validate_required(value: &str, field_name: &str) -> Result<(), ModelValidationError> {
    if value.trim().is_empty() {
        Err(ModelValidationError::new(
            ValidationErrorKind::RequiredField,
            field_name,
        ))
    } else {
        Ok(())
    }
}

/// Validate and normalize a path string
pub fn validate_path(path: Option<String>) -> Option<String> {
    path.map(|p| {
        let trimmed = p.trim();
        if trimmed.is_empty() {
            String::from("/")
        } else if !trimmed.starts_with('/') {
            format!("/{}", trimmed)
        } else {
            trimmed.to_string()
        }
    })
}

/// Validate and normalize a referrer URL
pub fn validate_referrer(referrer: Option<String>) -> Option<String> {
    referrer.and_then(|r| {
        let trimmed = r.trim();
        if trimmed.is_empty() || trimmed == "null" || trimmed == "undefined" {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_uuid_valid() {
        let valid_uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert!(validate_uuid(valid_uuid).is_ok());
        assert!(validate_uuid_field(valid_uuid, "test").is_ok());
        assert!(validate_uuid_regex(valid_uuid));
    }

    #[test]
    fn test_validate_uuid_invalid() {
        let invalid_uuids = vec![
            "not-a-uuid",
            "550e8400-e29b-41d4-a716",
            "550e8400-e29b-41d4-a716-446655440000-extra",
            "",
        ];

        for uuid in invalid_uuids {
            assert!(validate_uuid(uuid).is_err());
            assert!(validate_uuid_field(uuid, "test").is_err());
            assert!(!validate_uuid_regex(uuid));
        }
    }

    #[test]
    fn test_validate_event_type() {
        assert!(validate_event_type("CLICK").is_ok());
        assert!(validate_event_type("VIEW").is_ok());
        assert!(validate_event_type("PURCHASE").is_ok());
        assert!(validate_event_type("click").is_ok()); // Case insensitive
        assert!(validate_event_type("View").is_ok());

        assert!(validate_event_type("INVALID").is_err());
        assert!(validate_event_type("").is_err());
    }

    #[test]
    fn test_validate_rfc3339() {
        let valid_timestamps = vec![
            "2023-01-01T00:00:00Z",
            "2023-01-01T00:00:00+00:00",
            "2023-01-01T00:00:00.123Z",
            "2023-12-31T23:59:59-05:00",
        ];

        for ts in valid_timestamps {
            assert!(validate_rfc3339(ts).is_ok());
            assert!(validate_timestamp_field(ts, "test").is_ok());
        }

        let invalid_timestamps = vec!["2023-01-01", "2023-01-01 00:00:00", "not-a-timestamp", ""];

        for ts in invalid_timestamps {
            assert!(validate_rfc3339(ts).is_err());
            assert!(validate_timestamp_field(ts, "test").is_err());
        }
    }

    #[test]
    fn test_validate_amount() {
        assert_eq!(validate_amount(Some(100), "amount").unwrap(), Some(100));
        assert_eq!(validate_amount(Some(0), "amount").unwrap(), Some(0));
        assert_eq!(validate_amount(None, "amount").unwrap(), None);

        assert!(validate_amount(Some(-1), "amount").is_err());
        assert!(validate_amount(Some(-100), "amount").is_err());
    }

    #[test]
    fn test_validate_string_length() {
        assert!(validate_string_length("hello", "test", Some(1), Some(10)).is_ok());
        assert!(validate_string_length("hello", "test", Some(5), Some(5)).is_ok());

        assert!(validate_string_length("hi", "test", Some(3), None).is_err());
        assert!(validate_string_length("hello world", "test", None, Some(5)).is_err());
    }

    #[test]
    fn test_validate_required() {
        assert!(validate_required("value", "test").is_ok());
        assert!(validate_required(" value ", "test").is_ok());

        assert!(validate_required("", "test").is_err());
        assert!(validate_required("   ", "test").is_err());
    }

    #[test]
    fn test_validate_path() {
        assert_eq!(
            validate_path(Some("/home".to_string())),
            Some("/home".to_string())
        );
        assert_eq!(
            validate_path(Some("home".to_string())),
            Some("/home".to_string())
        );
        assert_eq!(
            validate_path(Some("  /about  ".to_string())),
            Some("/about".to_string())
        );
        assert_eq!(validate_path(Some("".to_string())), Some("/".to_string()));
        assert_eq!(validate_path(None), None);
    }

    #[test]
    fn test_validate_referrer() {
        assert_eq!(
            validate_referrer(Some("https://example.com".to_string())),
            Some("https://example.com".to_string())
        );
        assert_eq!(validate_referrer(Some("null".to_string())), None);
        assert_eq!(validate_referrer(Some("undefined".to_string())), None);
        assert_eq!(validate_referrer(Some("".to_string())), None);
        assert_eq!(validate_referrer(Some("   ".to_string())), None);
        assert_eq!(validate_referrer(None), None);
    }

    #[test]
    fn test_validate_optional_url() {
        assert!(validate_optional_url(Some("https://example.com"), "url").is_ok());
        assert!(validate_optional_url(Some("http://test.co.uk/path"), "url").is_ok());
        assert!(validate_optional_url(Some("example.com/path"), "url").is_ok());
        assert!(validate_optional_url(None, "url").is_ok());
        assert!(validate_optional_url(Some(""), "url").is_ok());

        assert!(validate_optional_url(Some("not a url"), "url").is_err());
        assert!(validate_optional_url(Some("ftp://test.com"), "url").is_err());
    }
}
