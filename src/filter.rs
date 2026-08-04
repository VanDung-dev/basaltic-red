// Bitmask Flags representing Audit Error Codes
pub const ERR_INVALID_PASSENGER: u64 = 1 << 0; // 0x01: Invalid passenger count
pub const ERR_INVALID_FARE: u64 = 1 << 1; // 0x02: Negative or zero fare amount
pub const ERR_INVALID_SPEED: u64 = 1 << 2; // 0x04: Unrealistic speed or distance/fare anomaly
