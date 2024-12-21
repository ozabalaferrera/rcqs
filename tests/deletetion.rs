#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
    use std::error::Error;
    use uuid::Uuid;

    #[test]
    fn delete_catalog() -> Result<(), Box<dyn Error>> {
        let mut client = test_utils::redis_client();
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
    fn delete_by_id() -> Result<(), Box<dyn Error>> {
        let mut client = test_utils::redis_client();
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
    fn delete_and_get_by_id() -> Result<(), Box<dyn Error>> {
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h, "equal item set and catalog hash entry count");

        let item_fetched = catalog.delete_and_get_by_id(&mut client, id)?;
        let item_fetched = item_fetched.expect("same item returned");

        assert_eq!(
            item_fetched.id(),
            id,
            "registered and fetched item IDs should match"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }

    #[test]
    fn delete_multiple_by_id() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = test_utils::redis_client();
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

    #[test]
    fn delete_and_get_multiple_by_id() -> Result<(), Box<dyn Error>> {
        const CNT: i64 = 100;
        let mut client = test_utils::redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let items: Vec<CatalogItem<String>> = (0..CNT).map(|_| test_utils::random_item()).collect();
        let mut ids: Vec<Uuid> = items.iter().map(|item| item.id()).collect();

        let (z, h) = catalog.register_multiple(&mut client, &items)?;
        assert_eq!(z, CNT, "{} checkout set entries", CNT);
        assert!(h, "true catalog hash entry result");

        let items_fetched = catalog.delete_and_get_multiple_by_id(&mut client, &ids)?;
        let mut ids_fetched: Vec<Uuid> = items_fetched.iter().map(|item| item.id()).collect();

        assert_eq!(
            items.len(),
            items_fetched.len(),
            "registered and fetched item count should match"
        );

        ids.sort();
        ids_fetched.sort();

        let matching = ids
            .iter()
            .zip(ids_fetched.iter())
            .filter(|&(id, id_checked_out)| *id == *id_checked_out)
            .count();

        assert_eq!(
            items.len(),
            matching,
            "registered and fetched item IDs should match"
        );

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0, "zero keys deleted");

        Ok(())
    }
}
