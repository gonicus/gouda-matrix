#[macro_export]
macro_rules! debug_assert_or_log {
    ($cond:expr, $($arg:tt)*) => {
        debug_assert!($cond, $($arg)*);
        if !$cond {
            log::error!(concat!("DEBUG_ASSERT: ", $($arg)*));
        }
    };
}

#[macro_export]
macro_rules! unwrap_or_log_return {
    ($expr:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(err) => {
                log::error!("{}: {err:?}", $msg);
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! unwrap_or_log_return_err {
    ($expr:expr, $msg:expr) => {
        match $expr {
            Ok(v) => v,
            Err(err) => {
                log::error!("{}: {err:?}", $msg);
                return Err(err.into());
            }
        }
    };
}

#[macro_export]
macro_rules! unwrap_or_log_return_option {
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
