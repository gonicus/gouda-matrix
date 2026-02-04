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
