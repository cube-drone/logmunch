use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use moka::future::Cache;
use growable_bloom_filter::GrowableBloom;
use anyhow::Result;

use crate::minute::{Minute, MinuteId};

pub struct MinuteDB{
    db: Arc<RwLock<HashMap<MinuteId, Arc<Mutex<Minute>>>>>,
    bloom_cache: Cache<MinuteId, Arc<GrowableBloom>>,
}

const ESTIMATED_BLOOM_SIZE: u64 = 1500000;

impl MinuteDB{
    pub fn new(cache_bytes: u64) -> MinuteDB{
        let entries = cache_bytes / ESTIMATED_BLOOM_SIZE;

        MinuteDB{
            db: Arc::new(RwLock::new(HashMap::new())),
            bloom_cache: Cache::new(entries),
        }
    }

    pub fn insert_no_cache(&self, minute: crate::minute::Minute) -> Result<()>{
        let key = minute.unique_id();
        self.db.write().unwrap().insert(key, Arc::new(Mutex::new(minute)));

        Ok(())
    }

    pub async fn insert(&self, minute: crate::minute::Minute) -> Result<()>{
        let key = minute.unique_id();
        let bloom = minute.get_bloom_filter()?;
        self.bloom_cache.insert(key.clone(), Arc::new(bloom)).await;
        self.db.write().unwrap().insert(key, Arc::new(Mutex::new(minute)));

        Ok(())
    }
}
