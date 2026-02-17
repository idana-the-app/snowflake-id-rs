use crate::defs::CLOCK_BACKWARDS_TOLERANCE_MS;
use crate::error::SnowflakeError;
use crate::snowflake::Snowflake;
use chrono::Utc;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;

pub enum SnowflakeOperation<S> {
    Ready(S),
    Pending(Duration),
}

struct GeneratorState {
    last_timestamp: i64,
    sequence: u64,
}

pub struct SnowflakeGenerator<S: Snowflake> {
    machine_id: u64,
    state: Mutex<GeneratorState>,
    epoch: i64,
    _marker: PhantomData<S>,
}

impl<S: Snowflake> SnowflakeGenerator<S> {
    /// Creates a new SnowflakeGenerator with a custom epoch
    ///
    /// # Arguments
    /// * `machine_id` - Unique machine/datacenter ID (0-1023)
    /// * `epoch` - Custom epoch in milliseconds since Unix epoch
    ///
    /// # Example
    /// ```
    /// use snowflake_id::SnowflakeGenerator;
    ///
    /// // Use a custom epoch (e.g., Jan 1, 2024)
    /// let generator = SnowflakeGenerator::with_epoch(1, 1704067200000).unwrap();
    /// ```
    pub fn with_epoch(machine_id: u64, epoch: i64) -> Result<Self, SnowflakeError> {
        if machine_id > S::max_machine_id() {
            return Err(SnowflakeError::InvalidMachineId(
                machine_id,
                S::max_machine_id(),
            ));
        }

        Ok(SnowflakeGenerator {
            machine_id,
            state: Mutex::new(GeneratorState {
                last_timestamp: 0,
                sequence: 0,
            }),
            epoch,
            _marker: PhantomData,
        })
    }

    /// Returns the epoch being used by this generator
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub fn try_next_id(&self) -> Result<SnowflakeOperation<S>, SnowflakeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SnowflakeError::GeneratorPoisoned)?;

        let timestamp = Self::current_timestamp();

        if timestamp < state.last_timestamp {
            let drift = state.last_timestamp - timestamp;
            if drift <= CLOCK_BACKWARDS_TOLERANCE_MS {
                return Ok(SnowflakeOperation::Pending(Duration::from_millis(
                    drift as u64,
                )));
            } else {
                return Err(SnowflakeError::ClockMovedBackwards);
            }
        }

        if timestamp == state.last_timestamp {
            let next_seq = (state.sequence + 1) & S::max_sequence();
            if next_seq == 0 {
                return Ok(SnowflakeOperation::Pending(Duration::from_millis(1)));
            }
            state.sequence = next_seq;
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = timestamp;

        let timestamp_offset = timestamp - self.epoch;
        if timestamp_offset < 0 || timestamp_offset > S::max_timestamp() {
            return Err(SnowflakeError::TimestampOverflow);
        }

        let masked_timestamp = (timestamp_offset as u64) & ((1u64 << S::timestamp_bits()) - 1);

        Ok(SnowflakeOperation::Ready(S::from_component_parts(
            masked_timestamp,
            self.machine_id,
            state.sequence,
        )))
    }

    pub fn next_id(&self, mut on_pending: impl FnMut(Duration)) -> S {
        loop {
            match self.try_next_id().expect("snowflake generation failed") {
                SnowflakeOperation::Ready(id) => return id,
                SnowflakeOperation::Pending(wait) => {
                    on_pending(wait);
                }
            }
        }
    }

    pub fn new(machine_id: u64) -> Result<Self, SnowflakeError> {
        Self::with_epoch(machine_id, crate::defs::SNOWFLAKE_ID_EPOCH)
    }

    pub fn next_id_bulk(
        &self,
        count: usize,
        mut on_pending: impl FnMut(Duration),
    ) -> Vec<S> {
        let mut ids = Vec::with_capacity(count);

        // Acquire lock once for the entire bulk operation
        let mut state = self
            .state
            .lock()
            .expect("snowflake generator mutex poisoned");

        for _ in 0..count {
            let mut timestamp = Self::current_timestamp();

            // Handle clock moving backwards with tolerance
            if timestamp < state.last_timestamp {
                let drift = state.last_timestamp - timestamp;
                if drift <= CLOCK_BACKWARDS_TOLERANCE_MS {
                    // Wait for clock to catch up (small NTP adjustment)
                    while timestamp < state.last_timestamp {
                        on_pending(Duration::from_millis(drift as u64));
                        timestamp = Self::current_timestamp();
                    }
                } else {
                    // Large backwards movement - fail immediately
                    panic!("clock moved backwards beyond tolerance");
                }
            }

            if timestamp == state.last_timestamp {
                state.sequence = (state.sequence + 1) & S::max_sequence();
                if state.sequence == 0 {
                    while timestamp <= state.last_timestamp {
                        on_pending(Duration::from_millis(1));
                        timestamp = Self::current_timestamp();
                    }
                }
            } else {
                state.sequence = 0;
            }

            state.last_timestamp = timestamp;

            // Calculate timestamp offset and validate it fits in timestamp bits
            let timestamp_offset = timestamp - self.epoch;
            if timestamp_offset < 0 || timestamp_offset > S::max_timestamp() {
                panic!("snowflake timestamp overflow");
            }

            // Mask to timestamp bits to ensure bit 63 is always 0 (keeping ID positive)
            let masked_timestamp = (timestamp_offset as u64) & ((1u64 << S::timestamp_bits()) - 1);

            // Construct Id
            ids.push(S::from_component_parts(
                masked_timestamp,
                self.machine_id,
                state.sequence,
            ));
        }

        ids
    }

    fn current_timestamp() -> i64 {
        Utc::now().timestamp_millis()
    }
}
