use crate::defs::CLOCK_BACKWARDS_TOLERANCE_MS;
use crate::error::SnowflakeError;
use crate::generator::SnowflakeOperation;
use crate::snowflake::Snowflake;
use chrono::Utc;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::Mutex;

struct GeneratorState {
    last_timestamp: i64,
    sequence: u64,
}

pub struct AsyncSnowflakeGenerator<S: Snowflake> {
    machine_id: u64,
    state: Mutex<GeneratorState>,
    epoch: i64,
    _marker: PhantomData<S>,
}

impl<S: Snowflake> AsyncSnowflakeGenerator<S> {
    pub fn new(machine_id: u64) -> Result<Self, SnowflakeError> {
        Self::with_epoch(machine_id, crate::defs::SNOWFLAKE_ID_EPOCH)
    }

    pub fn with_epoch(machine_id: u64, epoch: i64) -> Result<Self, SnowflakeError> {
        if machine_id > S::max_machine_id() {
            return Err(SnowflakeError::InvalidMachineId(
                machine_id,
                S::max_machine_id(),
            ));
        }

        Ok(AsyncSnowflakeGenerator {
            machine_id,
            state: Mutex::new(GeneratorState {
                last_timestamp: 0,
                sequence: 0,
            }),
            epoch,
            _marker: PhantomData,
        })
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub async fn try_next_id(&self) -> Result<SnowflakeOperation<S>, SnowflakeError> {
        let mut state = self.state.lock().await;
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

        let masked_timestamp =
            (timestamp_offset as u64) & ((1u64 << S::timestamp_bits()) - 1);

        Ok(SnowflakeOperation::Ready(S::from_component_parts(
            masked_timestamp,
            self.machine_id,
            state.sequence,
        )))
    }

    pub async fn next_id(&self) -> S {
        loop {
            match self.try_next_id().await.expect("snowflake generation failed") {
                SnowflakeOperation::Ready(id) => return id,
                SnowflakeOperation::Pending(wait) => {
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    pub async fn next_id_bulk(&self, count: usize) -> Vec<S> {
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(self.next_id().await);
        }
        ids
    }

    fn current_timestamp() -> i64 {
        Utc::now().timestamp_millis()
    }
}
