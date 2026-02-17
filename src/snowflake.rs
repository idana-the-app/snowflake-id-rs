use core::hash::Hash;

pub trait Snowflake:
    Copy + Clone + PartialOrd + Ord + PartialEq + Eq + Hash + std::fmt::Debug
{
    fn from_component_parts(timestamp_offset: u64, machine_id: u64, sequence: u64) -> Self;

    fn timestamp(&self) -> u64 {
        (self.id() >> Self::timestamp_shift()) & Self::timestamp_mask()
    }

    fn timestamp_with_epoch(&self, epoch: i64) -> i64 {
        (self.timestamp() as i64) + epoch
    }

    fn machine_id(&self) -> u64 {
        (self.id() >> Self::sequence_bits()) & Self::machine_id_mask()
    }

    fn sequence(&self) -> u64 {
        self.id() & Self::sequence_mask()
    }

    fn id(&self) -> u64;

    fn is_valid(&self) -> bool {
        (self.id() & !Self::valid_mask()) == 0
    }

    fn timestamp_mask() -> u64 {
        (1u64 << Self::timestamp_bits()) - 1
    }

    fn machine_id_mask() -> u64 {
        (1u64 << Self::machine_id_bits()) - 1
    }

    fn sequence_mask() -> u64 {
        (1u64 << Self::sequence_bits()) - 1
    }

    fn valid_mask() -> u64 {
        (Self::timestamp_mask() << Self::timestamp_shift())
            | (Self::machine_id_mask() << Self::sequence_bits())
            | Self::sequence_mask()
    }

    fn timestamp_bits() -> u64;
    fn machine_id_bits() -> u64;
    fn sequence_bits() -> u64;

    fn timestamp_shift() -> u64 {
        Self::machine_id_bits() + Self::sequence_bits()
    }

    fn max_timestamp() -> i64 {
        Self::timestamp_mask() as i64
    }

    fn max_machine_id() -> u64 {
        Self::machine_id_mask()
    }

    fn max_sequence() -> u64 {
        Self::sequence_mask()
    }
}
