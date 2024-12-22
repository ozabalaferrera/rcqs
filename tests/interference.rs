#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
    use redis::Commands;
    use std::{error::Error, num::NonZero};
    use uuid::Uuid;

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

    #[test]
    fn register_and_checkout_item_by_id() -> Result<(), Box<dyn Error>> {
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let n: i64 = client.hdel(catalog.catalog_key(), id.to_string())?;
        assert_eq!(n, 1, "interfered to delete item from catalog");

        let item = catalog
            .checkout_by_id(&mut client, id)
            .expect("ok result from redis");
        assert!(item.is_none(), "registered item externally removed");

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert_eq!(zi, 0, "zero expiration set entry");
        assert_eq!(zc, 0, "one checkout set entry");
        assert_eq!(h, 0, "one catalog hash entry");

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_checkout_multiple_items() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<String> = items.iter().map(|item| item.id().to_string()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let n: i64 = client.hdel(catalog.catalog_key(), &ids)?;
        assert_eq!(n, CNT, "interfered to delete multiple items from catalog");

        let items_checked_out = catalog
            .checkout_multiple(&mut client, NonZero::new(CNT as usize).unwrap())
            .expect("ok result from redis");

        assert_eq!(
            items_checked_out.len(),
            0,
            "registered items externally removed"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_checkout_multiple_items_by_id() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();
        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let n: i64 = client.hdel(catalog.catalog_key(), &id_strings)?;
        assert_eq!(n, CNT, "interfered to delete multiple items from catalog");

        let items_checked_out_present: Vec<CatalogItem<String>> = catalog
            .checkout_multiple_by_id(&mut client, &ids)
            .expect("ok result from redis")
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(
            items_checked_out_present.len(),
            0,
            "registered items externally removed"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }
}
