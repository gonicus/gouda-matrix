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

#[macro_export]
macro_rules! unwrap_or_log_return_option {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("Error: {e:?}");
                return None;
            }
        }
    };
    ($expr:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}: {e:?}", $msg);
                return None;
            }
        }
    };
}
