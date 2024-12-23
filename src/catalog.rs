use super::{expire::Expiration, item::CatalogItem};
use chrono::Utc;
use core::f64;
use redis::{ConnectionLike, RedisResult};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, marker::PhantomData, num::NonZero};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Catalog<I>
where
    I: Debug + Serialize + DeserializeOwned,
{
    root_namespace: String,
    name: String,
    catalog_key: String,
    item_expirations_key: String,
    checkout_expirations_key: String,
    default_item_expiration: Expiration,
    default_checkout_expiration: Expiration,
    _item_type: PhantomData<CatalogItem<I>>,
}

impl<I> Catalog<I>
where
    I: Debug + Serialize + DeserializeOwned,
{
    /// Create a new [`Catalog`] with its keys at a given root namespace and name.
    /// Requires a default item expiration and a default checkout expiration.
    pub fn new(
        root_namespace: String,
        name: String,
        default_item_expiration: Expiration,
        default_checkout_expiration: Expiration,
    ) -> Self {
        let catalog_ns = format!("{}:{}", root_namespace, name);
        let catalog_key = format!("{}:catalog", catalog_ns);
        let item_expirations_key = format!("{}:item-expirations", catalog_ns);
        let checkout_expirations_key = format!("{}:checkout-expirations", catalog_ns);

        Self {
            root_namespace,
            name,
            catalog_key,
            item_expirations_key,
            checkout_expirations_key,
            default_item_expiration,
            default_checkout_expiration,
            _item_type: PhantomData::<CatalogItem<I>>,
        }
    }

    /// Root namespace or prefix for keys related to this [`Catalog`].
    pub fn root_namespace(&self) -> &str {
        self.root_namespace.as_str()
    }

    /// Name of this [`Catalog`].
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Key for hash containing items.
    pub fn catalog_key(&self) -> &str {
        self.catalog_key.as_str()
    }

    /// Key for ordered set containing item expirations.
    pub fn catalog_expirations_key(&self) -> &str {
        self.item_expirations_key.as_str()
    }

    /// Key for ordered set containing checkout expirations.
    pub fn checkouts_expirations_key(&self) -> &str {
        self.checkout_expirations_key.as_str()
    }

    /// Default item expiration.
    pub fn default_item_expiration(&self) -> Expiration {
        self.default_item_expiration
    }

    /// Default checkout expiration.
    pub fn default_checkout_expiration(&self) -> Expiration {
        self.default_checkout_expiration
    }

    /// Delete all catalog keys from the database.
    pub fn destroy_catalog<C>(self, con: &mut C) -> RedisResult<i64>
    where
        C: ConnectionLike,
    {
        let keys = &[
            self.catalog_key,
            self.item_expirations_key,
            self.checkout_expirations_key,
        ];
        redis::transaction(con, keys, |trc, pipe| pipe.del(keys).query(trc)).map(|(n,)| n)
    }

    fn register_with_expiration_f64_timestamp<C>(
        &self,
        con: &mut C,
        item: CatalogItem<I>,
        expires_on: f64,
    ) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];
        let item_id = item.id.to_string();
        redis::transaction(con, keys, move |trc, pipe| {
            pipe.zadd(&self.item_expirations_key, &item_id, expires_on)
                .hset(&self.catalog_key, &item_id, &item)
                .query(trc)
        })
    }

    /// Register item using its expiration or the catalog's default if none.
    pub fn register<C>(&self, con: &mut C, item: CatalogItem<I>) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let expires_on = item
            .expires_on
            .unwrap_or_else(|| self.default_item_expiration.as_f64_timestamp());
        self.register_with_expiration_f64_timestamp(con, item, expires_on)
    }

    /// Register item using the provided expiration.
    pub fn register_with_expiration<C>(
        &self,
        con: &mut C,
        item: CatalogItem<I>,
        expiration: Expiration,
    ) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let expires_on = expiration.as_f64_timestamp();
        self.register_with_expiration_f64_timestamp(con, item, expires_on)
    }

    fn register_multiple_with_f64_timestamp_expirations<C>(
        &self,
        con: &mut C,
        items: &[CatalogItem<I>],
        expirations: &[f64],
    ) -> RedisResult<(i64, bool)>
    where
        C: ConnectionLike,
    {
        debug_assert_eq!(expirations.len(), items.len());

        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        let scores_members: Vec<(&f64, String)> = expirations
            .iter()
            .zip(items.iter().map(|item| item.id.to_string()))
            .collect();

        let item_kvs: Vec<(String, &CatalogItem<I>)> = items
            .iter()
            .map(|item| (item.id.to_string(), item))
            .collect();

        redis::transaction(con, keys, move |trc, pipe| {
            let result: RedisResult<(i64, String)> = pipe
                .zadd_multiple(&self.item_expirations_key, &scores_members)
                .hset_multiple(&self.catalog_key, &item_kvs)
                .query(trc);

            result.map(|(z, h)| Some((z, h == "OK")))
        })
    }

    /// Register items using their expiration or the catalog's default if none.
    pub fn register_multiple<C>(
        &self,
        con: &mut C,
        items: &[CatalogItem<I>],
    ) -> RedisResult<(i64, bool)>
    where
        C: ConnectionLike,
    {
        let default_expiration = self.default_item_expiration.as_f64_timestamp();
        let expirations: Vec<f64> = items
            .iter()
            .map(|item| item.expires_on.unwrap_or(default_expiration))
            .collect();

        self.register_multiple_with_f64_timestamp_expirations(con, items, &expirations)
    }

    /// Register items using the provided expiration.
    pub fn register_multiple_with_expiration<C>(
        &self,
        con: &mut C,
        items: &[CatalogItem<I>],
        expiration: Expiration,
    ) -> RedisResult<(i64, bool)>
    where
        C: ConnectionLike,
    {
        let expiration = expiration.as_f64_timestamp();
        let expirations = vec![expiration; items.len()];
        self.register_multiple_with_f64_timestamp_expirations(con, items, &expirations)
    }

    fn checkout_with_f64_timestamp_timeout<C>(
        &self,
        con: &mut C,
        timeout_on: f64,
    ) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (items_scores,): (Vec<(String, f64)>,) =
                pipe.zpopmin(&self.item_expirations_key, 1).query(trc)?;

            let result = if let Some((item_id, _item_expiration)) = items_scores.first() {
                pipe.clear();
                let (queried_item,): (Option<CatalogItem<I>>,) =
                    pipe.hget(&self.catalog_key, item_id).query(trc)?;

                if queried_item.is_some() {
                    pipe.clear();
                    let _: (i64,) = pipe
                        .zadd(&self.checkout_expirations_key, item_id, timeout_on)
                        .query(trc)?;
                }

                queried_item
            } else {
                None
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Checkout item using the catalog's default checkout timeout.
    pub fn checkout<C>(&self, con: &mut C) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = self.default_checkout_expiration.as_f64_timestamp();
        self.checkout_with_f64_timestamp_timeout(con, timeout_on)
    }

    /// Checkout item using the provided checkout timeout.
    pub fn checkout_with_timeout<C>(
        &self,
        con: &mut C,
        timeout: Expiration,
    ) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = timeout.as_f64_timestamp();
        self.checkout_with_f64_timestamp_timeout(con, timeout_on)
    }

    fn checkout_multiple_with_f64_timestamp_timeout<C>(
        &self,
        con: &mut C,
        count: NonZero<usize>,
        timeout_on: f64,
    ) -> RedisResult<Vec<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (item_expirations,): (Vec<(String, f64)>,) = pipe
                .zpopmin(&self.item_expirations_key, count.get() as isize)
                .query(trc)?;
            let item_ids: Vec<String> = item_expirations.into_iter().map(|(id, _)| id).collect();
            pipe.clear();
            let (queried_items,): (Vec<Option<CatalogItem<I>>>,) =
                pipe.hget(&self.catalog_key, &item_ids).query(trc)?;
            let found_items: Vec<CatalogItem<I>> = queried_items.into_iter().flatten().collect();

            if !found_items.is_empty() {
                pipe.clear();
                let scores_ids: Vec<(f64, String)> = found_items
                    .iter()
                    .map(|item| (timeout_on, item.id.to_string()))
                    .collect();

                let _: (String,) = pipe
                    .zadd_multiple(&self.checkout_expirations_key, &scores_ids)
                    .query(trc)?;
            }

            RedisResult::Ok(Some(found_items))
        })
    }

    /// Checkout items using the catalog's default checkout timeout.
    pub fn checkout_multiple<C>(
        &self,
        con: &mut C,
        count: NonZero<usize>,
    ) -> RedisResult<Vec<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = self.default_checkout_expiration.as_f64_timestamp();
        self.checkout_multiple_with_f64_timestamp_timeout(con, count, timeout_on)
    }

    /// Checkout items using the provided checkout timeout.
    pub fn checkout_multiple_with_timeout<C>(
        &self,
        con: &mut C,
        count: NonZero<usize>,
        timeout: Expiration,
    ) -> RedisResult<Vec<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = timeout.as_f64_timestamp();
        self.checkout_multiple_with_f64_timestamp_timeout(con, count, timeout_on)
    }

    fn checkout_by_id_with_f64_timestamp_timeout<C>(
        &self,
        con: &mut C,
        id: Uuid,
        timeout_on: f64,
    ) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];
        let item_id = id.to_string();

        redis::transaction(con, keys, |trc, pipe| {
            let (n,): (i64,) = pipe.zrem(&self.item_expirations_key, &item_id).query(trc)?;
            if n == 0 {
                return RedisResult::Ok(Some(None));
            }
            pipe.clear();

            let (queried_item,): (Option<CatalogItem<I>>,) =
                pipe.hget(&self.catalog_key, &item_id).query(trc)?;
            if queried_item.is_some() {
                pipe.clear();
                let _: (i64,) = pipe
                    .zadd(&self.checkout_expirations_key, &item_id, timeout_on)
                    .query(trc)?;
            }

            RedisResult::Ok(Some(queried_item))
        })
    }

    /// Checkout item by ID using the catalog's default checkout timeout.
    pub fn checkout_by_id<C>(&self, con: &mut C, id: Uuid) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = self.default_checkout_expiration.as_f64_timestamp();
        self.checkout_by_id_with_f64_timestamp_timeout(con, id, timeout_on)
    }

    /// Checkout item by ID using the provided checkout timeout.
    pub fn checkout_by_id_with_timeout<C>(
        &self,
        con: &mut C,
        id: Uuid,
        timeout: Expiration,
    ) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = timeout.as_f64_timestamp();
        self.checkout_by_id_with_f64_timestamp_timeout(con, id, timeout_on)
    }

    fn checkout_multiple_by_id_with_f64_timestamp_timeout<C>(
        &self,
        con: &mut C,
        ids: &[Uuid],
        timeout_on: f64,
    ) -> RedisResult<Vec<Option<CatalogItem<I>>>>
    where
        C: ConnectionLike,
    {
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];
        let item_ids: Vec<String> = ids.iter().map(|id| id.to_string()).collect();

        redis::transaction(con, keys, |trc, pipe| {
            let (scores,): (Vec<Option<f64>>,) = pipe
                .zscore_multiple(&self.item_expirations_key, &item_ids)
                .query(trc)?;
            pipe.clear();
            let _: (i64,) = pipe
                .zrem(&self.item_expirations_key, &item_ids)
                .query(trc)?;
            let found_ids: Vec<&String> = item_ids
                .iter()
                .zip(scores.iter())
                .filter_map(|(id, score)| score.map(|_| id))
                .collect();

            let result = if !found_ids.is_empty() {
                pipe.clear();
                let (queried_items,): (Vec<Option<CatalogItem<I>>>,) =
                    pipe.hget(&self.catalog_key, &found_ids).query(trc)?;
                let found_items: Vec<&CatalogItem<I>> = queried_items.iter().flatten().collect();
                if !found_items.is_empty() {
                    let scores_ids: Vec<(f64, String)> = found_items
                        .iter()
                        .map(|item| (timeout_on, item.id.to_string()))
                        .collect();

                    pipe.clear();
                    let _: (i64,) = pipe
                        .zadd_multiple(&self.checkout_expirations_key, &scores_ids)
                        .query(trc)?;
                }

                queried_items
            } else {
                Vec::new()
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Checkout items by ID using the catalog's default checkout timeout.
    pub fn checkout_multiple_by_id<C>(
        &self,
        con: &mut C,
        ids: &[Uuid],
    ) -> RedisResult<Vec<Option<CatalogItem<I>>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = self.default_checkout_expiration.as_f64_timestamp();
        self.checkout_multiple_by_id_with_f64_timestamp_timeout(con, ids, timeout_on)
    }

    /// Checkout items by ID using the provided checkout timeout.
    pub fn checkout_multiple_by_id_with_timeout<C>(
        &self,
        con: &mut C,
        ids: &[Uuid],
        timeout: Expiration,
    ) -> RedisResult<Vec<Option<CatalogItem<I>>>>
    where
        C: ConnectionLike,
    {
        let timeout_on = timeout.as_f64_timestamp();
        self.checkout_multiple_by_id_with_f64_timestamp_timeout(con, ids, timeout_on)
    }

    /// Query for and remove items that should be expired from the catalog.
    pub fn expire_items<C>(&self, con: &mut C) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let now = Utc::now();
        let ts = now.timestamp() as f64;
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (item_ids,): (Vec<String>,) = pipe
                .zrangebyscore(&self.item_expirations_key, 0, ts)
                .query(trc)?;

            let result: (i64, i64) = if !item_ids.is_empty() {
                pipe.clear();
                pipe.hdel(&self.catalog_key, &item_ids)
                    .zrem(&self.item_expirations_key, &item_ids)
                    .query(trc)?
            } else {
                (0, 0)
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Query for, remove, and return items that should be expired from the catalog.
    pub fn expire_and_get_items<C>(&self, con: &mut C) -> RedisResult<Vec<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let now = Utc::now();
        let ts = now.timestamp() as f64;
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (item_ids,): (Vec<String>,) = pipe
                .zrangebyscore(&self.item_expirations_key, f64::NEG_INFINITY, ts)
                .query(trc)?;

            let result = if !item_ids.is_empty() {
                pipe.clear();
                let (items,): (Vec<CatalogItem<I>>,) =
                    pipe.hget(&self.catalog_key, &item_ids).query(trc)?;
                pipe.clear();
                let _: (i64, i64) = pipe
                    .hdel(&self.catalog_key, &item_ids)
                    .zrem(&self.item_expirations_key, &item_ids)
                    .query(trc)?;

                items
            } else {
                Vec::new()
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Query for and return items whose checkout has timed out.
    ///
    /// If item is somehow missing an expiration timestamp, it will be set to
    /// the catalog's default timeout.
    pub fn timeout_checkouts<C>(&self, con: &mut C) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let now = Utc::now();
        let ts = now.timestamp() as f64;
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (checked_out_item_ids,): (Vec<String>,) = pipe
                .zrangebyscore(&self.checkout_expirations_key, f64::NEG_INFINITY, ts)
                .query(trc)?;

            let result = if !checked_out_item_ids.is_empty() {
                pipe.clear();
                let (items,): (Vec<Option<CatalogItem<I>>>,) = pipe
                    .hget(&self.catalog_key, &checked_out_item_ids)
                    .query(trc)?;
                let items: Vec<(&String, &CatalogItem<I>)> = checked_out_item_ids
                    .iter()
                    .zip(items.iter())
                    .filter_map(|(score, item)| item.as_ref().map(|item| (score, item)))
                    .collect();

                let expirations: Vec<(f64, &String)> = items
                    .iter()
                    .map(|(item_id, item)| {
                        let expires_on = item
                            .expires_on
                            .unwrap_or(self.default_item_expiration.as_f64_timestamp());
                        (expires_on, *item_id)
                    })
                    .collect();

                pipe.clear();
                pipe.zadd_multiple(&self.item_expirations_key, &expirations)
                    .zrem(&self.checkout_expirations_key, &checked_out_item_ids)
                    .query(trc)?
            } else {
                (0, 0)
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Relinquish a checked out item back to the catalog ahead of the checkout timeout.
    ///
    /// If item is somehow missing an expiration timestamp, it will be set to
    /// the catalog's default timeout.
    pub fn relinquish_by_id<C>(&self, con: &mut C, id: Uuid) -> RedisResult<(i64, i64)>
    where
        C: ConnectionLike,
    {
        let id = id.to_string();
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (zc,): (i64,) = pipe.zrem(&self.checkout_expirations_key, &id).query(trc)?;
            let result = if zc == 1 {
                pipe.clear();
                let (item,): (CatalogItem<I>,) = pipe.hget(&self.catalog_key, &id).query(trc)?;
                pipe.clear();
                let expires_on = item
                    .expires_on
                    .unwrap_or(self.default_item_expiration.as_f64_timestamp());
                let (zi,): (i64,) = pipe
                    .zadd(&self.item_expirations_key, &id, expires_on)
                    .query(trc)?;
                (zc, zi)
            } else {
                (0, 0)
            };

            RedisResult::Ok(Some(result))
        })
    }

    /// Delete an item from the catalog.
    pub fn delete_by_id<C>(&self, con: &mut C, id: Uuid) -> RedisResult<(i64, i64, i64)>
    where
        C: ConnectionLike,
    {
        let id = id.to_string();
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            pipe.zrem(&self.item_expirations_key, &id)
                .zrem(&self.checkout_expirations_key, &id)
                .hdel(&self.catalog_key, &id)
                .query(trc)
        })
    }

    /// Delete and get an item from the catalog.
    pub fn delete_and_get_by_id<C>(
        &self,
        con: &mut C,
        id: Uuid,
    ) -> RedisResult<Option<CatalogItem<I>>>
    where
        C: ConnectionLike,
    {
        let id = id.to_string();
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (_, _, item, _): (i64, i64, Option<CatalogItem<I>>, i64) = pipe
                .zrem(&self.item_expirations_key, &id)
                .zrem(&self.checkout_expirations_key, &id)
                .hget(&self.catalog_key, &id)
                .hdel(&self.catalog_key, &id)
                .query(trc)?;

            RedisResult::Ok(Some(item))
        })
    }

    /// Delete items from the catalog.
    pub fn delete_multiple_by_id<C>(
        &self,
        con: &mut C,
        ids: &[Uuid],
    ) -> RedisResult<(i64, i64, i64)>
    where
        C: ConnectionLike,
    {
        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            pipe.zrem(&self.item_expirations_key, &id_strings)
                .zrem(&self.checkout_expirations_key, &id_strings)
                .hdel(&self.catalog_key, &id_strings)
                .query(trc)
        })
    }

    /// Delete and get items from the catalog.
    pub fn delete_and_get_multiple_by_id<C>(
        &self,
        con: &mut C,
        ids: &[Uuid],
    ) -> RedisResult<Vec<Option<CatalogItem<I>>>>
    where
        C: ConnectionLike,
    {
        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        let keys = &[
            &self.catalog_key,
            &self.item_expirations_key,
            &self.checkout_expirations_key,
        ];

        redis::transaction(con, keys, |trc, pipe| {
            let (_, _, items, _): (i64, i64, Vec<Option<CatalogItem<I>>>, i64) = pipe
                .zrem(&self.item_expirations_key, &id_strings)
                .zrem(&self.checkout_expirations_key, &id_strings)
                .hget(&self.catalog_key, &id_strings)
                .hdel(&self.catalog_key, &id_strings)
                .query(trc)?;

            RedisResult::Ok(Some(items))
        })
    }
}
