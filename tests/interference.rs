#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
    use redis::Commands;
    use std::error::Error;

    #[test]
    fn register_and_checkout_item() -> Result<(), Box<dyn Error>> {
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let n: i64 = client.hdel(catalog.catalog_key(), id.to_string())?;
        assert_eq!(n, 1, "interfered to delete item from catalog");

        let item = catalog.checkout(&mut client).expect("ok result from redis");
        assert!(item.is_none(), "registered item externally removed");

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert_eq!(zi, 0, "zero expiration set entry");
        assert_eq!(zc, 0, "one checkout set entry");
        assert_eq!(h, 0, "one catalog hash entry");

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }
}
