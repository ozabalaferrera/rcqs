#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem, Expiration};
    use std::{error::Error, num::NonZero, thread::sleep, time::Duration};
    use uuid::Uuid;

    #[test]
    fn register_with_expiration_passed() -> Result<(), Box<dyn Error>> {
        let expiration: Expiration = Expiration::from_now_with_offset(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item_with_expiration(expiration);

        let (z, h) = catalog.register_with_expiration(&mut client, item, expiration)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        sleep(Duration::from_secs(2));

        let (z, h) = catalog.expire_items(&mut client)?;
        assert_eq!(z, 1, "expired one item");
        assert_eq!(z, h, "equal item set and catalog hash expiration count");

        let item = catalog.checkout(&mut client).expect("ok result from redis");
        assert!(item.is_none(), "registered item should have expired");

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn register_and_get_with_expiration_passed() -> Result<(), Box<dyn Error>> {
        let expiration: Expiration = Expiration::from_ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item_with_expiration(expiration);
        let id = item.id();

        let (z, h) = catalog.register_with_expiration(&mut client, item, expiration)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        sleep(Duration::from_secs(2));

        let items: Vec<CatalogItem<String>> = catalog.expire_and_get_items(&mut client)?;
        assert_eq!(items.len(), 1, "expired one item");
        assert_eq!(
            items.first().unwrap().id(),
            id,
            "expired and fetched registered item"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn register_multiple_with_expiration_passed() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let expiration: Expiration = Expiration::from_f64_ttl(1.9);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple_with_expiration(&mut client, &items, expiration)?;
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

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn checkout_by_id_with_timeout_passed() -> Result<(), Box<dyn Error>> {
        const TIMEOUT: Expiration = Expiration::Ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let item = catalog
            .checkout_by_id_with_timeout(&mut client, id, TIMEOUT)
            .expect("ok result from redis");
        assert!(
            item.is_some(),
            "registered item should have been checked out"
        );

        sleep(Duration::from_secs(2));

        let (zi, zc) = catalog.timeout_checkouts(&mut client)?;
        assert_eq!(zi, 1, "one checkout timed out");
        assert_eq!(zi, zc, "item set additions equals checkout set removals");

        let item = catalog
            .checkout_by_id_with_timeout(&mut client, id, TIMEOUT)
            .expect("ok result from redis");
        assert!(
            item.is_some(),
            "previous checkout expired and item checked out again"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn checkout_by_id_multiple_with_timeout_passed() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        const TIMEOUT: Expiration = Expiration::Ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_checked_out = catalog
            .checkout_multiple_by_id_with_timeout(&mut client, &ids, TIMEOUT)
            .expect("ok result from redis");
        assert_eq!(
            items_checked_out.len(),
            items.len(),
            "registered item should have been checked out"
        );

        sleep(Duration::from_secs(2));

        let (zi, zc) = catalog.timeout_checkouts(&mut client)?;
        assert_eq!(zi, CNT, "{} checkout timed out", CNT);
        assert_eq!(zi, zc, "item set additions equals checkout set removals");

        let items_checked_out = catalog
            .checkout_multiple_by_id_with_timeout(&mut client, &ids, TIMEOUT)
            .expect("ok result from redis");
        assert_eq!(
            items_checked_out.len(),
            items.len(),
            "previous checkouts expired and items checked out again"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn checkout_with_timeout_passed() -> Result<(), Box<dyn Error>> {
        const TIMEOUT: Expiration = Expiration::Ttl(1);

        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let item = catalog
            .checkout_with_timeout(&mut client, TIMEOUT)
            .expect("ok result from redis");
        assert!(
            item.is_some(),
            "registered item should have been checked out"
        );

        sleep(Duration::from_secs(2));

        let (zi, zc) = catalog.timeout_checkouts(&mut client)?;
        assert_eq!(zi, 1, "one checkout timed out");
        assert_eq!(zi, zc, "item set additions equals checkout set removals");

        let item = catalog
            .checkout_with_timeout(&mut client, TIMEOUT)
            .expect("ok result from redis");
        assert!(
            item.is_some(),
            "previous checkout expired and item checked out again"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn checkout_multiple_with_timeout_passed() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        const TIMEOUT: Expiration = Expiration::Ttl(1);

        let cnt_u = NonZero::new(CNT as usize).unwrap();
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_checked_out = catalog
            .checkout_multiple_with_timeout(&mut client, cnt_u, TIMEOUT)
            .expect("ok result from redis");
        assert_eq!(
            items_checked_out.len(),
            items.len(),
            "registered item should have been checked out"
        );

        sleep(Duration::from_secs(2));

        let (zi, zc) = catalog.timeout_checkouts(&mut client)?;
        assert_eq!(zi, CNT, "{} checkout timed out", CNT);
        assert_eq!(zi, zc, "item set additions equals checkout set removals");

        let items_checked_out = catalog
            .checkout_multiple_with_timeout(&mut client, cnt_u, TIMEOUT)
            .expect("ok result from redis");
        assert_eq!(
            items_checked_out.len(),
            items.len(),
            "previous checkouts expired and items checked out again"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }

    #[test]
    fn checkout_and_relinquish() -> Result<(), Box<dyn Error>> {
        const TIMEOUT: Expiration = Expiration::Ttl(1);

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

        sleep(Duration::from_secs(2));

        let item = catalog.checkout(&mut client).expect("ok result from redis");
        assert!(
            item.is_none(),
            "should not be able to check out the same item id again"
        );

        let (zc, zi) = catalog.relinquish_by_id(&mut client, id)?;
        assert_eq!(zi, 1, "one checkout relinquished");
        assert_eq!(zi, zc, "item set additions equals checkout set removals");

        let item = catalog
            .checkout_by_id_with_timeout(&mut client, id, TIMEOUT)
            .expect("ok result from redis");
        assert!(
            item.is_some(),
            "previous checkout relinquished and item checked out again"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2, "two keys deleted");

        Ok(())
    }
}
