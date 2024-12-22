use super::expire::Expiration;
use chrono::{TimeZone, Utc};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, FromRedisValue, ToRedisArgs)]
pub struct CatalogItem<I> {
    pub(crate) id: Uuid,
    pub(crate) contents: I,
    pub(crate) created_on: i64,
    pub(crate) expires_on: Option<f64>,
}

impl<I> CatalogItem<I>
where
    I: Debug + Serialize + DeserializeOwned,
{
    pub fn new(contents: I) -> Self {
        CatalogItem {
            id: Uuid::new_v4(),
            contents,
            created_on: Utc::now().timestamp(),
            expires_on: None,
        }
    }

    pub fn new_with_expiration(expiration: Expiration, contents: I) -> Self {
        CatalogItem {
            id: Uuid::new_v4(),
            contents,
            created_on: Utc::now().timestamp(),
            expires_on: Some(expiration.as_f64_timestamp()),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn contents(&self) -> &I {
        &self.contents
    }

    pub fn take_contents(self) -> I {
        self.contents
    }

    pub fn expires_on_f64_timestamp(&self) -> Option<f64> {
        self.expires_on
    }

    pub fn created_on(&self) -> Option<chrono::DateTime<Utc>> {
        Utc.timestamp_opt(self.created_on, 0).single()
    }
}
