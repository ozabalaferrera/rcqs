#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    extern crate test_utils;

    use rcqs::{Catalog, CatalogItem};
    use redis::Client;
    use std::error::Error;
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
        assert_eq!(z, h);

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2);

        Ok(())
    }

    #[test]
    fn register_and_checkout_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h);

        let item = catalog
            .checkout(&mut client)
            .expect("ok result from redis")
            .expect("item with ID as registered");
        assert_eq!(item.id(), id, "registered and fetch item IDs should match");

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
        assert_eq!(z, h);

        let item = catalog
            .checkout_by_id(&mut client, id)
            .expect("ok result from redis")
            .expect("item with ID as registered");
        assert_eq!(item.id(), id, "registered and fetch item IDs should match");

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
    fn register_and_delete_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog: Catalog<String> = test_utils::random_catalog();
        let item: CatalogItem<String> = test_utils::random_item();
        let id = item.id();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h);

        let (zi, zc, h) = catalog.delete_by_id(&mut client, id)?;
        assert!(zi == 1);
        assert!(zc == 0);
        assert!(h == 1);

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0);

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
        assert_eq!(z, CNT);
        assert!(h);

        let (zi, zc, h) = catalog.delete_multiple_by_id(&mut client, &ids)?;
        assert!(zi == CNT);
        assert!(zc == 0);
        assert!(h == CNT);

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 0);

        Ok(())
    }
}
