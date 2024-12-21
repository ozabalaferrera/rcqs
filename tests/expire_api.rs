use chrono::Utc;
use rcqs::Expiration;

#[test]
fn from_now_with_offset() {
    let expiration = Expiration::from_now_with_offset(60);
    assert!(
        match expiration {
            Expiration::Timestamp(t) => t > Utc::now().timestamp(),
            _ => false,
        },
        "expected timestamp greater than now"
    )
}

#[test]
fn from_timestamp() {
    let expiration = Expiration::from_timestamp(0);
    assert!(
        match expiration {
            Expiration::Timestamp(t) => t < Utc::now().timestamp(),
            _ => false,
        },
        "expected timestamp less than now"
    )
}

#[test]
fn from_f64_timestamp() {
    let expiration = Expiration::from_f64_timestamp(0.5);
    assert!(
        match expiration {
            Expiration::Timestamp(t) => t == 0,
            _ => false,
        },
        "expected timestamp equal 0"
    )
}

#[test]
fn from_ttl() {
    let expiration = Expiration::from_ttl(3);
    assert!(
        match expiration {
            Expiration::Ttl(t) => t == 3,
            _ => false,
        },
        "expected TTL equal 3"
    )
}

#[test]
fn from_f64_ttl() {
    let expiration = Expiration::from_f64_ttl(3.9);
    assert!(
        match expiration {
            Expiration::Ttl(t) => t == 3,
            _ => false,
        },
        "expected TTL equal 3"
    )
}

#[test]
fn as_f64_timestamp() {
    let expiration = Expiration::from_f64_timestamp(f64::NEG_INFINITY).as_f64_timestamp();
    assert_eq!(
        expiration,
        f64::INFINITY,
        "expected never conversion of negative to positive infinity"
    )
}

#[test]
fn display() {
    println!("Expiration never: {}", Expiration::Never);
    println!("Expiration ttl 1 second: {}", Expiration::Ttl(1));
    println!(
        "Expiration timestamp 1 second from now: {}",
        Expiration::from_now_with_offset(1)
    );
}
