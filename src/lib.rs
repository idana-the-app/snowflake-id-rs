use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "sqlx")]
use sqlx::Type;

pub mod defs;
pub mod error;
pub mod generator;
pub mod snowflake;

#[cfg(feature = "tokio")]
pub mod async_generator;

pub use defs::*;
use error::SnowflakeError;
pub use snowflake::Snowflake;

/// Type alias — the concrete generator is now the generic one parameterised on `SnowflakeId`.
pub type SnowflakeGenerator = generator::SnowflakeGenerator<SnowflakeId>;

#[cfg(feature = "tokio")]
pub type AsyncSnowflakeGenerator = async_generator::AsyncSnowflakeGenerator<SnowflakeId>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "sqlx", derive(Type))]
#[cfg_attr(feature = "sqlx", sqlx(type_name = "BIGINT"))]
#[cfg_attr(feature = "sqlx", sqlx(transparent))]
pub struct SnowflakeId(i64);

// ---------------------------------------------------------------------------
// Snowflake trait implementation
// ---------------------------------------------------------------------------

impl Snowflake for SnowflakeId {
    fn from_component_parts(timestamp_offset: u64, machine_id: u64, sequence: u64) -> Self {
        let id = (timestamp_offset << Self::timestamp_shift())
            | (machine_id << Self::sequence_bits())
            | sequence;
        SnowflakeId(id as i64)
    }

    fn id(&self) -> u64 {
        self.0 as u64
    }

    fn timestamp_bits() -> u64 {
        TIMESTAMP_BITS
    }

    fn machine_id_bits() -> u64 {
        MACHINE_ID_BITS
    }

    fn sequence_bits() -> u64 {
        SEQUENCE_BITS
    }
}

// ---------------------------------------------------------------------------
// Inherent methods (preserves existing API with i64 return types)
// ---------------------------------------------------------------------------

impl SnowflakeId {
    pub fn new(value: i64) -> Result<Self, SnowflakeError> {
        if value < 0 {
            return Err(SnowflakeError::InvalidId(
                "Snowflake ID cannot be negative".to_string(),
            ));
        }
        Ok(SnowflakeId(value))
    }

    /// Creates a SnowflakeId without validation. Only use this if you're certain the value is valid.
    ///
    /// # Safety
    /// The caller must ensure that the value is non-negative.
    pub fn new_unchecked(value: i64) -> Self {
        SnowflakeId(value)
    }

    pub fn id(&self) -> i64 {
        self.0
    }

    /// Returns the timestamp offset (in milliseconds) stored in this snowflake ID.
    /// This is NOT a Unix timestamp. To get the actual Unix timestamp, use `timestamp_with_epoch()`.
    pub fn timestamp(&self) -> i64 {
        <Self as Snowflake>::timestamp(self) as i64
    }

    /// Returns the timestamp in milliseconds since Unix epoch, using a custom epoch
    pub fn timestamp_with_epoch(&self, epoch: i64) -> i64 {
        <Self as Snowflake>::timestamp_with_epoch(self, epoch)
    }

    pub fn machine_id(&self) -> u64 {
        <Self as Snowflake>::machine_id(self)
    }

    pub fn sequence(&self) -> u64 {
        <Self as Snowflake>::sequence(self)
    }
}

// ---------------------------------------------------------------------------
// FromStr
// ---------------------------------------------------------------------------

impl FromStr for SnowflakeId {
    type Err = SnowflakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s
            .parse::<i64>()
            .map_err(|e| SnowflakeError::InvalidId(format!("Failed to parse: {}", e)))?;

        if value < 0 {
            return Err(SnowflakeError::InvalidId(
                "Snowflake ID cannot be negative".to_string(),
            ));
        }

        Ok(SnowflakeId(value))
    }
}

// ---------------------------------------------------------------------------
// Display, TryFrom, Into, Serde
// ---------------------------------------------------------------------------

impl fmt::Display for SnowflakeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<i64> for SnowflakeId {
    type Error = SnowflakeError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        SnowflakeId::new(value)
    }
}

impl From<SnowflakeId> for i64 {
    fn from(id: SnowflakeId) -> Self {
        id.0
    }
}

impl Serialize for SnowflakeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.0.to_string())
        } else {
            serializer.serialize_i64(self.0)
        }
    }
}

impl<'de> Deserialize<'de> for SnowflakeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SnowflakeIdVisitor;

        impl<'de> serde::de::Visitor<'de> for SnowflakeIdVisitor {
            type Value = SnowflakeId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or integer representing a snowflake id")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value > i64::MAX as u64 {
                    return Err(E::custom("snowflake id value exceeds i64::MAX"));
                }
                Ok(SnowflakeId::new_unchecked(value as i64))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value < 0 {
                    Err(E::custom("snowflake id cannot be negative"))
                } else {
                    Ok(SnowflakeId::new_unchecked(value))
                }
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| E::custom("invalid snowflake id string"))?;
                if parsed < 0 {
                    return Err(E::custom("snowflake id cannot be negative"));
                }
                Ok(SnowflakeId::new_unchecked(parsed))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(SnowflakeIdVisitor)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::thread;

    #[test]
    fn test_snowflake_generator() {
        let generator = SnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();
        let id1 = generator.next_id(|_| thread::yield_now());
        let id2 = generator.next_id(|_| thread::yield_now());

        assert_ne!(id1, id2);
        assert!(id1.id() < id2.id());
    }

    #[test]
    fn test_snowflake_id_components() {
        let generator = SnowflakeGenerator::with_epoch(42, SNOWFLAKE_ID_EPOCH).unwrap();
        let id = generator.next_id(|_| thread::yield_now());

        assert_eq!(id.machine_id(), 42);
        // timestamp() returns offset, not Unix timestamp
        assert!(id.timestamp() > 0);
        // timestamp_with_epoch() returns actual Unix timestamp
        assert!(id.timestamp_with_epoch(SNOWFLAKE_ID_EPOCH) > SNOWFLAKE_ID_EPOCH);
    }

    #[test]
    fn test_bulk_generation() {
        let generator = SnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();
        let ids = generator.next_id_bulk(100, |_| thread::yield_now());

        assert_eq!(ids.len(), 100);

        for i in 1..ids.len() {
            assert!(ids[i - 1].id() < ids[i].id());
        }
    }

    #[test]
    fn test_serialization() {
        let id = SnowflakeId::new(123456789012345678).unwrap();

        let json_string = serde_json::to_string(&id).unwrap();
        assert_eq!(json_string, "\"123456789012345678\"");

        let deserialized: SnowflakeId = serde_json::from_str(&json_string).unwrap();
        assert_eq!(id, deserialized);

        let from_int: SnowflakeId = serde_json::from_str("123456789012345678").unwrap();
        assert_eq!(id, from_int);
    }

    #[test]
    fn test_display() {
        let id = SnowflakeId::new(987654321098765432).unwrap();
        assert_eq!(format!("{}", id), "987654321098765432");
    }

    #[test]
    fn test_invalid_machine_id() {
        let result = SnowflakeGenerator::with_epoch(MAX_MACHINE_ID + 1, SNOWFLAKE_ID_EPOCH);
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_epoch() {
        // Use Jan 1, 2024 as custom epoch
        let custom_epoch = 1704067200000i64;
        let generator = SnowflakeGenerator::with_epoch(5, custom_epoch).unwrap();

        assert_eq!(generator.epoch(), custom_epoch);

        let id = generator.next_id(|_| thread::yield_now());

        // Verify machine ID is correct
        assert_eq!(id.machine_id(), 5);

        // Verify timestamp() returns offset (not Unix timestamp)
        let offset = id.timestamp();
        assert!(offset > 0);
        // Offset should be reasonable (much less than MAX_TIMESTAMP_MS ~= 69 years)
        assert!(offset < MAX_TIMESTAMP_MS);

        // Verify timestamp_with_epoch() returns actual Unix timestamp
        let timestamp = id.timestamp_with_epoch(custom_epoch);
        assert!(timestamp >= custom_epoch);
        // Verify it's not too far in the future (less than ~69 years from custom epoch)
        assert!(timestamp < custom_epoch + MAX_TIMESTAMP_MS);

        // Verify ID is positive
        assert!(id.id() > 0);
    }

    #[test]
    fn test_custom_epoch_vs_default() {
        let custom_epoch = 1704067200000i64; // Jan 1, 2024
        let generator_custom = SnowflakeGenerator::with_epoch(1, custom_epoch).unwrap();
        let generator_default = SnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();

        let id_custom = generator_custom.next_id(|_| thread::yield_now());
        let id_default = generator_default.next_id(|_| thread::yield_now());

        // Both should be positive
        assert!(id_custom.id() > 0);
        assert!(id_default.id() > 0);

        // Timestamps extracted with correct epochs should be recent
        let ts_custom = id_custom.timestamp_with_epoch(custom_epoch);
        let ts_default = id_default.timestamp_with_epoch(SNOWFLAKE_ID_EPOCH);

        assert!(ts_custom >= custom_epoch);
        assert!(ts_default >= SNOWFLAKE_ID_EPOCH);
    }

    #[test]
    fn test_custom_epoch_uniqueness() {
        let custom_epoch = 1704067200000i64;
        let generator = SnowflakeGenerator::with_epoch(3, custom_epoch).unwrap();

        let id1 = generator.next_id(|_| thread::yield_now());
        let id2 = generator.next_id(|_| thread::yield_now());
        let id3 = generator.next_id(|_| thread::yield_now());

        // All IDs should be unique
        assert_ne!(id1.id(), id2.id());
        assert_ne!(id2.id(), id3.id());
        assert_ne!(id1.id(), id3.id());

        // IDs should be monotonically increasing
        assert!(id1.id() < id2.id());
        assert!(id2.id() < id3.id());
    }

    #[test]
    fn test_from_str_rejects_negative() {
        let result = SnowflakeId::from_str("-123");
        assert!(result.is_err());

        match result {
            Err(SnowflakeError::InvalidId(msg)) => {
                assert!(msg.contains("cannot be negative"));
            }
            _ => panic!("Expected InvalidId error"),
        }
    }

    #[test]
    fn test_from_str_rejects_invalid() {
        let result = SnowflakeId::from_str("not_a_number");
        assert!(result.is_err());

        match result {
            Err(SnowflakeError::InvalidId(msg)) => {
                assert!(msg.contains("Failed to parse"));
            }
            _ => panic!("Expected InvalidId error"),
        }
    }

    #[test]
    fn test_from_str_accepts_valid() {
        let result = SnowflakeId::from_str("123456789012345678");
        assert!(result.is_ok());

        let id = result.unwrap();
        assert_eq!(id.id(), 123456789012345678);
    }

    #[test]
    fn test_from_str_accepts_zero() {
        let result = SnowflakeId::from_str("0");
        assert!(result.is_ok());

        let id = result.unwrap();
        assert_eq!(id.id(), 0);
    }

    #[test]
    fn test_new_rejects_negative() {
        let result = SnowflakeId::new(-123);
        assert!(result.is_err());

        match result {
            Err(SnowflakeError::InvalidId(msg)) => {
                assert!(msg.contains("cannot be negative"));
            }
            _ => panic!("Expected InvalidId error"),
        }
    }

    #[test]
    fn test_try_from_rejects_negative() {
        use std::convert::TryFrom;
        let result = SnowflakeId::try_from(-456i64);
        assert!(result.is_err());
    }

    #[test]
    fn test_try_from_accepts_valid() {
        use std::convert::TryFrom;
        let result = SnowflakeId::try_from(123456789012345678i64);
        assert!(result.is_ok());

        let id = result.unwrap();
        assert_eq!(id.id(), 123456789012345678);
    }

    #[cfg(feature = "tokio")]
    mod async_tests {
        use super::*;
        use crate::generator::SnowflakeOperation;

        #[tokio::test]
        async fn test_async_generate() {
            let generator = AsyncSnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();
            let id1 = generator.next_id().await;
            let id2 = generator.next_id().await;

            assert_ne!(id1, id2);
            assert!(id1.id() < id2.id());
        }

        #[tokio::test]
        async fn test_async_generate_bulk() {
            let generator = AsyncSnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();
            let ids = generator.next_id_bulk(100).await;

            assert_eq!(ids.len(), 100);

            for i in 1..ids.len() {
                assert!(ids[i - 1].id() < ids[i].id());
            }
        }

        #[tokio::test]
        async fn test_async_try_next_id() {
            let generator = AsyncSnowflakeGenerator::with_epoch(1, SNOWFLAKE_ID_EPOCH).unwrap();
            let result = generator.try_next_id().await.unwrap();

            match result {
                SnowflakeOperation::Ready(id) => {
                    assert!(id.id() > 0);
                    assert_eq!(id.machine_id(), 1);
                }
                SnowflakeOperation::Pending(_) => panic!("Expected Ready, got Pending"),
            }
        }
    }
}
