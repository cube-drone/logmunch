use std::sync::{Arc, RwLock, Mutex};
use std::time::SystemTime;
use std::collections::{HashSet, BTreeMap};
use growable_bloom_filter::GrowableBloom;
use anyhow::Result;

use crate::minute_id::MinuteId;
use crate::minute::Minute;

pub struct MinuteDB{
    db: Arc<RwLock<BTreeMap<MinuteId, Arc<Mutex<Minute>>>>>,
    bloom_cache: Arc<RwLock<BTreeMap<MinuteId, Arc<GrowableBloom>>>>,
    data_directory: String,
    n_minutes: u64,
}

impl MinuteDB{
    pub fn new(n_minutes: u64, data_directory: String) -> MinuteDB{

        MinuteDB{
            db: Arc::new(RwLock::new(BTreeMap::new())),
            bloom_cache: Arc::new(RwLock::new(BTreeMap::new())),
            data_directory: data_directory,
            n_minutes: n_minutes,
        }
    }

    pub fn update(&self, new_list: HashSet<MinuteId>) -> Result<()> {
        let mut db = self.db.write().unwrap();
        let mut bloom_cache = self.bloom_cache.write().unwrap();

        let existing_keys = db.keys().cloned().collect::<HashSet<MinuteId>>();
        for key in existing_keys{
            if !new_list.contains(&key) {
                db.remove(&key);
                bloom_cache.remove(&key);
            }
        }
        for key in new_list{
            if db.contains_key(&key) == false {
                let minute = Minute::new(key.day, key.hour, key.minute, &key.unique_id, &self.data_directory)?;
                match minute.is_sealed(){
                    Ok(true) => {},
                    Ok(false) => {
                        // this minute isn't sealed yet, so we shouldn't read it
                        continue;
                    },
                    Err(e) => {
                        println!("Error checking if minute is sealed: {:?}", e);
                    }
                }
                let bloom = minute.get_bloom_filter()?;
                bloom_cache.insert(key.clone(), Arc::new(bloom));
                db.insert(key, Arc::new(Mutex::new(minute)));
            }
        }

        Ok(())
    }

    pub fn read_loop(&self){
        // 10 seconds (in microseconds)
        let interval_us = 10 * 1000000;

        loop {
            // start a timer
            let now = SystemTime::now();

            // read from disk and insert into db
            let files = crate::file_list::FileInfo::scan_and_clean(&self.data_directory, self.n_minutes).unwrap();
            let set_of_minutes: HashSet<MinuteId> = files.iter().map(|f| f.to_minute_id()).collect();
            match self.update(set_of_minutes){
                Ok(_) => {},
                Err(e) => {
                    println!("Error updating minute db: {:?}", e);
                }
            }

            // how long did that take?
            let elapsed = now.elapsed().unwrap();
            let elapsed_us = elapsed.as_micros() as i128;
            let sleep_us = interval_us - elapsed_us;

            // if we took too long, just skip the sleep
            if sleep_us < 0 {
                println!("Warning: write thread took too long: {} us", elapsed_us);
                continue;
            }
            else{
                std::thread::sleep(std::time::Duration::from_micros(sleep_us as u64));
            }
        }
    }
}
