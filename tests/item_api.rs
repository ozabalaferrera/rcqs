use rcqs::{CatalogItem, Expiration};
use uuid::Uuid;

#[test]
fn getters() {
    const EXPIRATION: Expiration = Expiration::Never;

    let content = Uuid::new_v4().to_string();
    let item = CatalogItem::new_with_expiration(EXPIRATION, content.clone());

    assert!(!item.id().to_string().is_empty());
    assert_eq!(item.contents(), &content);
    assert_eq!(item.expires_on_f64_timestamp(), Some(Expiration::NEVER));
    assert!(item.created_on().is_some());
    assert!(item.first_checkout_on().is_none());
    assert!(item.latest_checkout_on().is_none());
    assert_eq!(item.checkout_count(), 0);
    assert_eq!(item.take_contents(), content);
}
