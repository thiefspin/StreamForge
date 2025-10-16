//! Event data models for StreamForge
//!
//! This module defines the core event structures used throughout the pipeline,
//! including raw events from Kafka and normalized events for storage.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;
use validator::Validate;

use super::error::{ValidationError, ValidationErrorKind, ValidationErrors};
use super::validation::{
    validate_amount, validate_event_type, validate_path, validate_referrer, validate_rfc3339,
    validate_timestamp_field, validate_uuid, validate_uuid_field,
};

/// Event types supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// User clicked on something
    #[serde(rename = "CLICK")]
    Click,
    /// User viewed a page
    #[serde(rename = "VIEW")]
    View,
    /// User made a purchase
    #[serde(rename = "PURCHASE")]
    Purchase,
}

impl EventType {
    /// Parse event type from string
    pub fn from_str(s: &str) -> Result<Self, ValidationError> {
        match s.to_uppercase().as_str() {
            "CLICK" => Ok(EventType::Click),
            "VIEW" => Ok(EventType::View),
            "PURCHASE" => Ok(EventType::Purchase),
            _ => Err(ValidationError::with_context(
                ValidationErrorKind::InvalidEventType,
                "event_type",
                format!("Unknown event type: {}", s),
            )),
        }
    }

    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Click => "CLICK",
            EventType::View => "VIEW",
            EventType::Purchase => "PURCHASE",
        }
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Raw event as received from Kafka
///
/// This structure represents the raw JSON data from the Kafka topic
/// before any normalization or enrichment.
#[derive(Debug, Clone, Deserialize, Serialize, Validate)]
pub struct RawEvent {
    /// Unique event identifier
    #[validate(custom(function = "validate_uuid"))]
    pub event_id: String,

    /// Type of event (CLICK, VIEW, PURCHASE)
    #[validate(custom(function = "validate_event_type"))]
    pub event_type: String,

    /// When the event occurred (RFC3339 format)
    #[validate(custom(function = "validate_rfc3339"))]
    pub occurred_at: String,

    /// User who triggered the event
    #[validate(custom(function = "validate_uuid"))]
    pub user_id: String,

    /// Optional amount in cents (for purchase events)
    #[validate(range(min = 0))]
    pub amount_cents: Option<i64>,

    /// Optional page path where event occurred
    pub path: Option<String>,

    /// Optional referrer URL
    pub referrer: Option<String>,
}

impl RawEvent {
    /// Create a new raw event (mainly for testing)
    pub fn new(event_id: String, event_type: String, occurred_at: String, user_id: String) -> Self {
        Self {
            event_id,
            event_type,
            occurred_at,
            user_id,
            amount_cents: None,
            path: None,
            referrer: None,
        }
    }

    /// Validate all fields without using the validator crate
    pub fn validate_fields(&self) -> Result<(), ValidationErrors> {
        let mut errors = ValidationErrors::new();

        // Validate event_id
        if let Err(e) = validate_uuid_field(&self.event_id, "event_id") {
            errors.add(e);
        }

        // Validate event_type
        if let Err(e) = EventType::from_str(&self.event_type) {
            errors.add(e);
        }

        // Validate occurred_at
        if let Err(e) = validate_timestamp_field(&self.occurred_at, "occurred_at") {
            errors.add(e);
        }

        // Validate user_id
        if let Err(e) = validate_uuid_field(&self.user_id, "user_id") {
            errors.add(e);
        }

        // Validate amount_cents
        if let Err(e) = validate_amount(self.amount_cents, "amount_cents") {
            errors.add(e);
        }

        errors.into_result(())
    }
}

/// Normalized event ready for database storage
///
/// This structure represents the processed and validated event
/// with parsed types and additional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedEvent {
    /// Unique event identifier
    pub event_id: Uuid,

    /// Type of event
    pub event_type: EventType,

    /// When the event occurred
    pub occurred_at: DateTime<Utc>,

    /// User who triggered the event
    pub user_id: Option<Uuid>,

    /// Optional amount in cents
    pub amount_cents: Option<i64>,

    /// Optional page path (normalized)
    pub path: Option<String>,

    /// Optional referrer URL (cleaned)
    pub referrer: Option<String>,

    /// Kafka partition this event came from
    pub kafka_partition: i32,

    /// Kafka offset for this event
    pub kafka_offset: i64,

    /// When this event was processed
    pub processed_at: DateTime<Utc>,
}

impl NormalizedEvent {
    /// Set Kafka metadata
    pub fn with_kafka_metadata(mut self, partition: i32, offset: i64) -> Self {
        self.kafka_partition = partition;
        self.kafka_offset = offset;
        self
    }

    /// Check if this is a purchase event
    pub fn is_purchase(&self) -> bool {
        self.event_type == EventType::Purchase
    }

    /// Get the amount in dollars (if present)
    pub fn amount_dollars(&self) -> Option<f64> {
        self.amount_cents.map(|cents| cents as f64 / 100.0)
    }
}

/// Transform a raw event into a normalized event
impl TryFrom<RawEvent> for NormalizedEvent {
    type Error = ValidationError;

    fn try_from(raw: RawEvent) -> Result<Self, Self::Error> {
        // Validate all fields first
        raw.validate_fields().map_err(|e| {
            ValidationError::with_context(
                ValidationErrorKind::Custom(e.to_string()),
                "event",
                "Multiple validation errors occurred",
            )
        })?;

        // Parse and transform fields
        let event_id = validate_uuid_field(&raw.event_id, "event_id")?;
        let event_type = EventType::from_str(&raw.event_type)?;
        let occurred_at = validate_timestamp_field(&raw.occurred_at, "occurred_at")?;
        let user_id = if raw.user_id.is_empty() {
            None
        } else {
            Some(validate_uuid_field(&raw.user_id, "user_id")?)
        };
        let amount_cents = validate_amount(raw.amount_cents, "amount_cents")?;

        // Normalize optional fields
        let path = validate_path(raw.path);
        let referrer = validate_referrer(raw.referrer);

        // Validate purchase events have an amount
        if event_type == EventType::Purchase && amount_cents.is_none() {
            return Err(ValidationError::with_context(
                ValidationErrorKind::RequiredField,
                "amount_cents",
                "Purchase events must have an amount",
            ));
        }

        Ok(NormalizedEvent {
            event_id,
            event_type,
            occurred_at,
            user_id,
            amount_cents,
            path,
            referrer,
            kafka_partition: 0, // Will be set when processing from Kafka
            kafka_offset: 0,    // Will be set when processing from Kafka
            processed_at: Utc::now(),
        })
    }
}

/// Builder for creating test events
#[cfg(test)]
pub struct EventBuilder {
    event_id: String,
    event_type: String,
    occurred_at: String,
    user_id: String,
    amount_cents: Option<i64>,
    path: Option<String>,
    referrer: Option<String>,
}

#[cfg(test)]
impl EventBuilder {
    pub fn new() -> Self {
        Self {
            event_id: Uuid::new_v4().to_string(),
            event_type: "CLICK".to_string(),
            occurred_at: Utc::now().to_rfc3339(),
            user_id: Uuid::new_v4().to_string(),
            amount_cents: None,
            path: None,
            referrer: None,
        }
    }

    pub fn event_type(mut self, event_type: &str) -> Self {
        self.event_type = event_type.to_string();
        self
    }

    pub fn amount_cents(mut self, amount: i64) -> Self {
        self.amount_cents = Some(amount);
        self
    }

    pub fn path(mut self, path: &str) -> Self {
        self.path = Some(path.to_string());
        self
    }

    pub fn referrer(mut self, referrer: &str) -> Self {
        self.referrer = Some(referrer.to_string());
        self
    }

    pub fn build(self) -> RawEvent {
        RawEvent {
            event_id: self.event_id,
            event_type: self.event_type,
            occurred_at: self.occurred_at,
            user_id: self.user_id,
            amount_cents: self.amount_cents,
            path: self.path,
            referrer: self.referrer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(EventType::from_str("CLICK").unwrap(), EventType::Click);
        assert_eq!(EventType::from_str("view").unwrap(), EventType::View);
        assert_eq!(
            EventType::from_str("Purchase").unwrap(),
            EventType::Purchase
        );
        assert!(EventType::from_str("INVALID").is_err());
    }

    #[test]
    fn test_event_type_display() {
        assert_eq!(EventType::Click.to_string(), "CLICK");
        assert_eq!(EventType::View.to_string(), "VIEW");
        assert_eq!(EventType::Purchase.to_string(), "PURCHASE");
    }

    #[test]
    fn test_raw_event_validation() {
        let valid_event = EventBuilder::new().build();
        assert!(valid_event.validate_fields().is_ok());

        let invalid_event = RawEvent::new(
            "not-a-uuid".to_string(),
            "INVALID".to_string(),
            "not-a-timestamp".to_string(),
            "not-a-uuid".to_string(),
        );
        assert!(invalid_event.validate_fields().is_err());
    }

    #[test]
    fn test_raw_to_normalized_conversion() {
        let raw = EventBuilder::new()
            .event_type("PURCHASE")
            .amount_cents(1500)
            .path("/checkout")
            .referrer("https://example.com")
            .build();

        let normalized = NormalizedEvent::try_from(raw).unwrap();
        assert_eq!(normalized.event_type, EventType::Purchase);
        assert_eq!(normalized.amount_cents, Some(1500));
        assert_eq!(normalized.amount_dollars(), Some(15.0));
        assert_eq!(normalized.path, Some("/checkout".to_string()));
        assert!(normalized.is_purchase());
    }

    #[test]
    fn test_purchase_requires_amount() {
        let raw = EventBuilder::new().event_type("PURCHASE").build();
        let result = NormalizedEvent::try_from(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_path_normalization() {
        let test_cases = vec![
            (Some("home".to_string()), Some("/home".to_string())),
            (Some("/about".to_string()), Some("/about".to_string())),
            (
                Some("  /contact  ".to_string()),
                Some("/contact".to_string()),
            ),
            (Some("".to_string()), Some("/".to_string())),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let raw = EventBuilder::new().build();
            let mut normalized = NormalizedEvent::try_from(raw).unwrap();
            normalized.path = validate_path(input);
            assert_eq!(normalized.path, expected);
        }
    }

    #[test]
    fn test_referrer_cleaning() {
        let test_cases = vec![
            (
                Some("https://google.com".to_string()),
                Some("https://google.com".to_string()),
            ),
            (Some("null".to_string()), None),
            (Some("undefined".to_string()), None),
            (Some("   ".to_string()), None),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let raw = EventBuilder::new().build();
            let mut normalized = NormalizedEvent::try_from(raw).unwrap();
            normalized.referrer = validate_referrer(input);
            assert_eq!(normalized.referrer, expected);
        }
    }

    #[test]
    fn test_kafka_metadata() {
        let raw = EventBuilder::new().build();
        let normalized = NormalizedEvent::try_from(raw).unwrap().with_kafka_metadata(2, 12345);

        assert_eq!(normalized.kafka_partition, 2);
        assert_eq!(normalized.kafka_offset, 12345);
    }

    #[test]
    fn test_negative_amount_validation() {
        let raw = EventBuilder::new().event_type("PURCHASE").amount_cents(-100).build();

        let result = NormalizedEvent::try_from(raw);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_serialization() {
        let raw = EventBuilder::new().event_type("VIEW").path("/products").build();

        let json = serde_json::to_string(&raw).unwrap();
        assert!(json.contains("\"event_type\":\"VIEW\""));
        assert!(json.contains("\"path\":\"/products\""));

        let deserialized: RawEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.event_type, "VIEW");
        assert_eq!(deserialized.path, Some("/products".to_string()));
    }
}
