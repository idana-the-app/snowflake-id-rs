use std::fmt;

#[derive(Debug, Clone)]
pub enum SnowflakeError {
    InvalidMachineId(u64, u64),
    ClockMovedBackwards,
    TimestampOverflow,
    GeneratorPoisoned,
    InvalidId(String),
}

impl fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnowflakeError::InvalidMachineId(id, max) => {
                write!(
                    f,
                    "Invalid machine ID: {}. Must be between 0 and {}",
                    id, max
                )
            }
            SnowflakeError::ClockMovedBackwards => {
                write!(f, "Clock moved backwards. Refusing to generate id")
            }
            SnowflakeError::TimestampOverflow => {
                write!(
                    f,
                    "Timestamp exceeds maximum."
                )
            }
            SnowflakeError::GeneratorPoisoned => {
                write!(f, "ID generator mutex was poisoned by a panicking thread")
            }
            SnowflakeError::InvalidId(msg) => {
                write!(f, "Invalid snowflake ID: {}", msg)
            }
        }
    }
}

impl std::error::Error for SnowflakeError {}