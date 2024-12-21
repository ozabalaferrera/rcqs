#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem, Expiration};
    use std::{error::Error, thread::sleep, time::Duration};
    use uuid::Uuid;

    #[test]
    fn register_with_expiration_passed() -> Result<(), Box<dyn Error>> {
        const EXPIRATION: Expiration = Expiration::Ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item_with_expiration(EXPIRATION);

        let (z, h) = catalog.register_with_expiration(&mut client, item, EXPIRATION)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        sleep(Duration::from_secs(2));

        let (z, h) = catalog.expire_items(&mut client)?;
        assert_eq!(z, 1, "expired one item");
        assert_eq!(z, h, "equal item set and catalog hash expiration count");

        let item = catalog.checkout(&mut client).expect("ok result from redis");
        assert!(item.is_none(), "registered item should have expired");

        Ok(())
    }

    #[test]
    fn register_multiple_with_expiration_passed() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        const EXPIRATION: Expiration = Expiration::Ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple_with_expiration(&mut client, &items, EXPIRATION)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        sleep(Duration::from_secs(2));

        let (z, h) = catalog.expire_items(&mut client)?;
        assert_eq!(z, CNT, "expired {} items", CNT);
        assert_eq!(z, h, "equal item set and catalog hash expiration count");

        let item = catalog
            .checkout_multiple_by_id(&mut client, &ids)
            .expect("ok result from redis");
        assert!(item.is_empty(), "registered items should have expired");

        Ok(())
    }
}
