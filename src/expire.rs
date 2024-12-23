use chrono::{TimeZone, Utc};
use std::fmt::{Debug, Display};

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub enum Expiration {
    #[default]
    Never,
    Timestamp(i64),
    Ttl(i64),
}

impl Expiration {
    pub const NEVER: f64 = f64::INFINITY;

    pub fn from_now_with_offset(offset_seconds: i64) -> Expiration {
        Expiration::Timestamp(Utc::now().timestamp() + offset_seconds)
    }

    pub fn from_timestamp(timestamp: i64) -> Expiration {
        Expiration::Timestamp(timestamp)
    }

    pub fn from_f64_timestamp(timestamp: f64) -> Self {
        if f64::is_finite(timestamp) {
            Expiration::Timestamp(timestamp.trunc() as i64)
        } else {
            Expiration::Never
        }
    }

    pub fn from_ttl(seconds: i64) -> Expiration {
        Expiration::Ttl(seconds)
    }

    pub fn from_f64_ttl(seconds: f64) -> Self {
        if f64::is_finite(seconds) {
            Expiration::Ttl(seconds.trunc() as i64)
        } else {
            Expiration::Never
        }
    }

    pub fn as_f64_timestamp(&self) -> f64 {
        match self {
            Expiration::Never => f64::INFINITY,
            Expiration::Timestamp(ts) => *ts as f64,
            Expiration::Ttl(ttl) => (*ttl + Utc::now().timestamp()) as f64,
        }
    }
}

impl Display for Expiration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expiration::Never => write!(f, "never"),
            Expiration::Timestamp(s) => write!(f, "{}", Utc.timestamp_opt(*s, 0).unwrap()),
            Expiration::Ttl(s) => write!(f, "{s}"),
        }
    }
}
