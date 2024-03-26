use std::sync::{Arc, RwLock, Mutex};
use std::collections::{HashSet, BTreeMap};
use moka::future::Cache;
use growable_bloom_filter::GrowableBloom;
use anyhow::Result;

use crate::minute_id::MinuteId;
use crate::minute::Minute;

pub struct MinuteDB{
    db: Arc<RwLock<BTreeMap<MinuteId, Arc<Mutex<Minute>>>>>,
    bloom_cache: Cache<MinuteId, Arc<GrowableBloom>>,
    data_directory: String,
}

const ESTIMATED_BLOOM_SIZE: u64 = 1500000;

impl MinuteDB{
    pub fn new(cache_bytes: u64, data_directory: String) -> MinuteDB{
        let entries = cache_bytes / ESTIMATED_BLOOM_SIZE;

        MinuteDB{
            db: Arc::new(RwLock::new(BTreeMap::new())),
            bloom_cache: Cache::new(entries),
            data_directory: data_directory,
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

    pub async fn remove(&self, key: &MinuteId) -> Result<()>{
        self.bloom_cache.remove(key).await;
        self.db.write().unwrap().remove(key);

        Ok(())
    }

    pub fn get(&self, key: &MinuteId) -> Option<Arc<Mutex<Minute>>>{
        self.db.read().unwrap().get(key).cloned()
    }

    pub async fn update(&self, new_list: HashSet<MinuteId>) -> Result<()> {
        let mut db = self.db.write().unwrap();

        let existing_keys = db.keys().cloned().collect::<HashSet<MinuteId>>();
        for key in existing_keys{
            if !new_list.contains(&key) {
                db.remove(&key);
                self.bloom_cache.remove(&key).await;
            }
        }
        for key in new_list{
            if db.contains_key(&key) == false {

                let minute = Minute::new(key.day, key.hour, key.minute, &key.unique_id, &self.data_directory)?;
                let bloom = minute.get_bloom_filter()?;
                self.bloom_cache.insert(key.clone(), Arc::new(bloom)).await;
                db.insert(key, Arc::new(Mutex::new(minute)));
            }
        }

        Ok(())
    }
}
