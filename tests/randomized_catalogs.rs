#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
    use redis::Client;
    use std::{error::Error, num::NonZero};
    use uuid::Uuid;

    fn redis_client() -> Client {
        let url = format!("redis://{}:{}/", env!("REDIS_HOST"), env!("REDIS_PORT"));
        redis::Client::open(url).expect("valid redis url")
    }

    #[test]
    fn delete_catalog() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, 1, "one item set entry");
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_checkout_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let item = catalog
            .checkout(&mut client)
            .expect("ok result from redis")
            .expect("item with ID as registered");
        assert_eq!(item.id(), id, "registered and fetch item IDs should match");

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert_eq!(zi, 0, "zero expiration set entry");
        assert_eq!(zc, 1, "one checkout set entry");
        assert_eq!(h, 1, "one catalog hash entry");

        assert!(
            catalog
                .checkout_by_id(&mut client, id)
                .expect("ok result from redis")
                .is_none(),
            "should not be able to check out the same item id again"
        );

        assert!(
            catalog
                .checkout(&mut client)
                .expect("ok result from redis")
                .is_none(),
            "should have no items left to checkout"
        );

        Ok(())
    }

    #[test]
    fn register_and_checkout_item_by_id() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let item = catalog
            .checkout_by_id(&mut client, id)
            .expect("ok result from redis")
            .expect("item with ID as registered");
        assert_eq!(item.id(), id, "registered and fetch item IDs should match");

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert!(zi == 0, "zero expiration set entry");
        assert!(zc == 1, "one checkout set entry");
        assert!(h == 1, "one catalog hash entry");

        assert!(
            catalog
                .checkout_by_id(&mut client, id)
                .expect("ok result from redis")
                .is_none(),
            "should not be able to check out the same item id again"
        );

        assert!(
            catalog
                .checkout(&mut client)
                .expect("ok result from redis")
                .is_none(),
            "should have no items left to checkout"
        );

        Ok(())
    }

    #[test]
    fn register_and_checkout_multiple_items() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;

        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_checked_out = catalog
            .checkout_multiple(&mut client, NonZero::new(CNT as usize).unwrap())
            .expect("ok result from redis");

        assert_eq!(
            items.len(),
            items_checked_out.len(),
            "registered and fetched item count should match"
        );

        let found = items_checked_out
            .iter()
            .filter(|item| ids.contains(&item.id()))
            .count();

        assert_eq!(
            items.len(),
            found,
            "registered and fetched item IDs should match"
        );

        Ok(())
    }

    #[test]
    fn register_and_checkout_multiple_items_by_id() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_checked_out = catalog
            .checkout_multiple_by_id(&mut client, &ids)
            .expect("ok result from redis");

        assert_eq!(
            items.len(),
            items_checked_out.len(),
            "registered and fetched item count should match"
        );

        let matching = items
            .iter()
            .zip(items_checked_out.iter())
            .filter(|&(a, b)| a.id() == b.id())
            .count();

        assert_eq!(
            items.len(),
            matching,
            "registered and fetched item IDs should match"
        );

        Ok(())
    }

    #[test]
    fn register_and_delete_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert!(zi == 1, "one expiration set entry");
        assert!(zc == 0, "zero checkout set entry");
        assert!(h == 1, "one catalog hash entry");

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_delete_multiple_items() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let (zi, zc, h) = catalog.delete_multiple_by_id(&mut client, &ids)?;
        assert!(zi == CNT, "{} expiration set entry", CNT);
        assert!(zc == 0, "zero checkout set entry");
        assert!(h == CNT, "{} catalog hash entry", CNT);

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }
}
