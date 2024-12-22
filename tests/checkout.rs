#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
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

        let item = catalog
            .checkout(&mut client)
            .expect("ok result from redis")
            .expect("registered and checked out item");
        assert_eq!(
            item.id(),
            id,
            "registered and fetched item IDs should match"
        );

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
        let mut ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_checked_out = catalog
            .checkout_multiple(&mut client, NonZero::new(CNT as usize).unwrap())
            .expect("ok result from redis");
        let mut ids_checked_out: Vec<Uuid> =
            items_checked_out.iter().map(|item| item.id()).collect();

        assert_eq!(
            items.len(),
            items_checked_out.len(),
            "registered and fetched item count should match"
        );

        ids.sort();
        ids_checked_out.sort();

        let matching = ids
            .iter()
            .zip(ids_checked_out.iter())
            .filter(|&(id, id_checked_out)| *id == *id_checked_out)
            .count();

        assert_eq!(
            items.len(),
            matching,
            "registered and fetched item IDs should match"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_checkout_multiple_items_by_id() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = test_utils::redis_client();
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

        let matching = ids
            .iter()
            .zip(items_checked_out.iter().flatten())
            .filter(|&(id, item)| *id == item.id())
            .count();

        assert_eq!(
            items.len(),
            matching,
            "registered and fetched item IDs should match"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }
}
