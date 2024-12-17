mod fixtures;

#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    use crate::fixtures::{self};
    use rcqs::CatalogItem;
    use redis::Client;
    use std::{error::Error, sync::LazyLock};

    static mut CLIENT: LazyLock<Client> = LazyLock::new(redis_client);

    fn redis_client() -> Client {
        let url = format!("redis://{}:{}/", env!("REDIS_HOST"), env!("REDIS_PORT"));
        redis::Client::open(url).expect("valid redis url")
    }

    #[test]
    fn add_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let item = CatalogItem::<u32>::new(42);
        let id = item.id();

        let (z, h) = fixtures::CATALOG_USE.register(&mut client, item)?;
        assert_eq!(z, h);

        let (zi, zc, h) = fixtures::CATALOG_USE.delete_by_id(&mut client, id)?;
        assert!(zi == 1);
        assert!(zc == 0);
        assert!(h == 1);

        Ok(())
    }
}
