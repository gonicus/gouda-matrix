use std::time::{SystemTime, UNIX_EPOCH};

/// Gets the current unix timestamp in seconds.
pub fn get_unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
