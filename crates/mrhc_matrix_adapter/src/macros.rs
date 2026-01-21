#[macro_export]
macro_rules! unwrap_or_log_return {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("Error: {e:?}");
                return;
            }
        }
    };
}
