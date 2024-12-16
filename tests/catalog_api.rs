use rcqs::{Catalog, Expiration};

#[test]
fn getters() {
    const ROOT_NAMESPACE: &str = "rcqs:testing";
    const NAME: &str = "catalog-getters";
    const ITEM_EXPIRATION: Expiration = Expiration::Never;
    const CHECKOUT_EXPIRATION: Expiration = Expiration::Ttl(30);

    const CATALOG_KEY: &str = constcat::concat!(ROOT_NAMESPACE, ":", NAME);

    let catalog: Catalog<u32> = Catalog::new(
        ROOT_NAMESPACE.to_owned(),
        NAME.to_owned(),
        ITEM_EXPIRATION,
        CHECKOUT_EXPIRATION,
    );

    assert_eq!(catalog.root_namespace(), ROOT_NAMESPACE);
    assert_eq!(catalog.name(), NAME);
    assert!(catalog.catalog_key().starts_with(CATALOG_KEY));
    assert!(catalog.catalog_expirations_key().starts_with(CATALOG_KEY));
    assert!(catalog.checkouts_expirations_key().starts_with(CATALOG_KEY));
    assert_eq!(catalog.default_item_expiration(), ITEM_EXPIRATION);
    assert_eq!(catalog.default_checkout_expiration(), CHECKOUT_EXPIRATION);
}
