mod util;

#[test_with::env(REDIS_HOST, REDIS_PORT)]
mod with_client {
    use crate::util;
    use redis::Client;
    use std::error::Error;

    fn redis_client() -> Client {
        let url = format!("redis://{}:{}/", env!("REDIS_HOST"), env!("REDIS_PORT"));
        redis::Client::open(url).expect("valid redis url")
    }

    #[test]
    fn delete_catalog() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog = util::random_catalog();
        let item = util::random_item();

        let (z, h) = catalog.register(&mut client, item)?;
        assert_eq!(z, h);

        let n = catalog.destroy_catalog(&mut client)?;
        assert_eq!(n, 2);

        Ok(())
    }

    #[test]
    fn add_item() -> Result<(), Box<dyn Error>> {
        let mut client = redis_client();
        let catalog = util::random_catalog();
        let item = util::random_item();
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
}
