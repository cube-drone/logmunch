use std::time::SystemTime;
use std::fs;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use fxhash::FxHashSet as HashSet;
use growable_bloom_filter::GrowableBloom;
use postcard;

use rusqlite::{Connection as SqlConnection, DatabaseName, params, Transaction};

use crate::minute_id::MinuteId;

///
/// The Event is the basic unit of data that we store in a minute, it's a _log line_.
/// Maybe that means it should be renamed, "log line".
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log{
    pub id: i64,
    pub message: String,
    pub time: i64,
    pub host: String,
}

// Minute isn't intended to be passed around between threads, so it's not Sync, or Send, or nothin'
pub struct Minute{
    id: MinuteId,
    connection: SqlConnection,
}

const CREATE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS log (
    id INTEGER PRIMARY KEY,
    batch INTEGER,
    log TEXT NOT NULL,
    host TEXT NOT NULL,
    host_time INTEGER NOT NULL
)"#;

const INDEX_TIME: &str = r#"CREATE INDEX IF NOT EXISTS log_host_time ON log (host_time)"#;
const INDEX_HOST: &str = r#"CREATE INDEX IF NOT EXISTS log_host ON log (host)"#;
const INDEX_BATCH: &str = r#"CREATE INDEX IF NOT EXISTS log_batch ON log (batch)"#;

const INSERT_LOG: &str = r#"INSERT INTO log (id, batch, log, host, host_time) VALUES (?, ?, ?, ?, ?)"#;

const GET_LOG_BY_BATCH: &str = r#"SELECT id, log, host, host_time FROM log WHERE batch = ?"#;

const CREATE_SEARCH_FRAGMENTS: &str = r#"CREATE TABLE IF NOT EXISTS search_fragments (
    id INTEGER PRIMARY KEY,
    batch INTEGER,
    fragment TEXT,
    min_log_id INTEGER,
    max_log_id INTEGER
)"#;

const LIST_BATCHES: &str = r#"SELECT DISTINCT batch FROM log"#;
const TEST_FOR_FRAGMENT_IN_BATCH: &str = r#"SELECT COUNT(*) FROM search_fragments WHERE batch = ? AND fragment = ?"#;

const INDEX_FRAGMENT: &str = r#"CREATE INDEX IF NOT EXISTS search_fragments_fragment ON search_fragments (fragment)"#;
const INDEX_FRAGMENT_BATCH: &str = r#"CREATE INDEX IF NOT EXISTS search_fragments_batch ON search_fragments (batch)"#;

const INSERT_FRAGMENT: &str = r#"INSERT INTO search_fragments (id, batch, fragment) VALUES (?, ?, ?)"#;

const GET_FRAGMENTS: &str = r#"SELECT DISTINCT fragment FROM search_fragments"#;

const CREATE_BLOOM: &str = r#"CREATE TABLE IF NOT EXISTS bloom (
    id INTEGER PRIMARY KEY,
    bloom BLOB
)"#;

const INSERT_BLOOM: &str = r#"INSERT INTO bloom (id, bloom) VALUES (?, ?)"#;

const GET_BLOOM: &str = r#"SELECT bloom FROM bloom ORDER BY id ASC LIMIT 1"#;

const HAS_BLOOM: &str = r#"SELECT COUNT(*) FROM bloom"#;

impl Minute{
    pub fn new(day: u32, hour: u32, minute: u32, unique_id: &str, data_directory: &str) -> Result<Self> {

        let fullpath = format!("{}/{}/{}", data_directory, day, hour);
        let minutepath = format!("{}/{}/{}/{}-{}.db", data_directory, day, hour, minute, unique_id);

        fs::create_dir_all(fullpath)?;

        let connection = SqlConnection::open(minutepath)?;

        // Set the journal mode and synchronous mode: WAL and normal
        // (WAL is write-ahead logging, which is faster and more reliable than the default rollback journal)
        // (normal synchronous mode is the best choice for WAL, and is the best tradeoff between speed and reliability)
        // (we might even need to disable that to JUICE WRITE TIMES, but we'll see how it goes first)
        connection.pragma_update(Some(DatabaseName::Main), "journal_mode", "WAL")?;
        connection.pragma_update(Some(DatabaseName::Main), "synchronous", "normal")?;

        Self::execute_and_eat_already_exists_errors(&connection, CREATE_TABLE)?;
        Self::execute_and_eat_already_exists_errors(&connection, CREATE_SEARCH_FRAGMENTS)?;
        Self::execute_and_eat_already_exists_errors(&connection, CREATE_BLOOM)?;

        Ok(Minute{
            connection,
            id: MinuteId::new(day, hour, minute, unique_id),
        })
    }

    pub fn unique_id(&self) -> MinuteId {
        self.id.clone()
    }

    ///
    /// We know that CREATE TABLE IF NOT EXISTS will usually fail (the table will already exist), so we eat the error
    ///
    pub fn execute_and_eat_already_exists_errors(connection: &SqlConnection, sql: &str) -> Result<()> {
        match connection.execute(sql, []){
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("there is already") {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Could not execute SQL: {}", e))
                }
            }
        }
    }


    pub fn explode(fragments: &mut HashSet<String>, data: &String){
        // this hashset contains every word in the string
        // it also contains every 3-letter fragment of every word
        for word in data.split_whitespace() {
            let mut vec = Vec::new();
            for char in word.chars() {
                vec.push(char);
                let l =  vec.len();
                if l > 2 {
                    // push the last 3 characters of the vec
                    let str: String = vec[l-3..].iter().collect();
                    fragments.insert(str.to_lowercase());
                }
            }
        }
    }

    fn write_events_to_transaction(tx: &Transaction, data: Vec<crate::WritableEvent>) -> Result<()> {
        let mut statement = tx.prepare_cached(INSERT_LOG)?;
        let mut fragment_statement = tx.prepare_cached(INSERT_FRAGMENT)?;
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64;
        let batch = timestamp;
        let mut sequence = 0;
        let mut fragments: HashSet<String> = HashSet::default();
        // Lock the connection
        for event in data {
            //self.bytes += event.get_size_in_bytes() as u32;
            Minute::explode(&mut fragments, &event.event);
            fragments.insert(event.host.clone());

            let id = (timestamp * 1000000) + sequence as i64;
            sequence += 1;

            statement.execute(params![id, batch, event.event, event.host, event.time])?;
        }
        // remove the empty string, nobody wants that
        //fragments.remove("");
        for fragment in fragments {
            sequence += 1;
            let id = (timestamp * 1000000) + sequence as i64;
            fragment_statement.execute(params![id, batch, fragment])?;
        }
        Ok(())
    }

    pub fn write_second(&mut self, data: Vec<crate::WritableEvent>) -> Result<()> {
        //self.count += data.len() as u32;
        let tx = self.connection.transaction()?;
        Self::write_events_to_transaction(&tx, data)?;
        tx.commit()?;
        Ok(())
    }

    pub fn generate_bloom_filter(&mut self) -> Result<()> {
        let mut statement = self.connection.prepare_cached(GET_FRAGMENTS)?;
        let mut gbloom = GrowableBloom::new(0.01, 1000000);
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let fragment: String = row.get(0)?;
            gbloom.insert(fragment);
        }

        let postcard_serialized = postcard::to_allocvec(&gbloom)?;
        let size_bytes = postcard_serialized.len();
        println!("Bloom filter size: {} bytes", size_bytes);

        let mut statement = self.connection.prepare_cached(INSERT_BLOOM)?;
        let timestamp_micros = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64;
        statement.execute(params![timestamp_micros, postcard_serialized])?;

        Ok(())
    }

    pub fn seal(&mut self) -> Result<()>{
        // once we seal the minute, we shouldn't write to it anymore
        // (and why would we? it's in the past)
        self.connection.execute(INDEX_TIME, [])?;
        self.connection.execute(INDEX_HOST, [])?;
        self.connection.execute(INDEX_BATCH, [])?;
        self.connection.execute(INDEX_FRAGMENT, [])?;
        self.connection.execute(INDEX_FRAGMENT_BATCH, [])?;

        // generate the bloooooooom
        self.generate_bloom_filter()?;

        self.connection.execute("VACUUM", [])?;

        Ok(())
    }

    pub fn is_sealed(&self) -> Result<bool> {
        let mut statement = self.connection.prepare_cached(HAS_BLOOM)?;
        let mut rows = statement.query([])?;
        let count: i64 = rows.next()?.unwrap().get(0)?;
        Ok(count > 0)
    }

    pub fn get_bloom_filter(&self) -> Result<GrowableBloom> {
        let mut statement = self.connection.prepare_cached(GET_BLOOM)?;
        let mut rows = statement.query([])?;
        let blob: Vec<u8> = rows.next()?.unwrap().get(0)?;
        let bloom: GrowableBloom = postcard::from_bytes(&blob)?;
        Ok(bloom)
    }

    pub fn search(&self, search: &crate::search_token::Search) -> Result<Vec<Log>> {
        //
        // BEFORE the search function is called, we've already verified that the minute
        //  contains the search term (probably) using the bloom filter.
        // Now it's time to actually search the minute for the term.
        //

        // first, get a list of all of the batches in the minute
        let mut statement = self.connection.prepare_cached(LIST_BATCHES)?;
        let mut rows = statement.query([])?;
        let mut batches = HashSet::default();
        while let Some(row) = rows.next()? {
            let batch: i64 = row.get(0)?;
            batches.insert(batch);
        }

        let mut results: Vec<Log> = Vec::new();

        // determine which batches are likely to contain the search term
        for batch_id in batches{
            let batch_contains_search = search.lambda_test(&|set| {
                // for each batch, we can try to disqualify the batch by finding a fragment that doesn't match
                let mut test_statement = self.connection.prepare_cached(TEST_FOR_FRAGMENT_IN_BATCH).unwrap();
                for fragment in set {
                    let resp = test_statement.query_row(params![batch_id, fragment], |row| {
                        let count: i64 = row.get(0)?;
                        Ok(count)
                    });
                    if resp.unwrap() == 0 {
                        //println!("Batch {} does not contain fragment {}", batch_id, fragment);
                        return false;
                    }
                    else{
                        //println!("Batch {} contains fragment {}", batch_id, fragment);
                    }
                }
                true
            });
            if !batch_contains_search {
                continue;
            }
            // if we can't disqualify the batch, we can search the batch for the search term
            let mut statement = self.connection.prepare_cached(GET_LOG_BY_BATCH)?;
            let mut rows = statement.query(params![batch_id])?;
            while let Some(row) = rows.next()? {
                let host: String = row.get(2)?;
                let message: String = row.get(1)?;
                let search_string = format!("{} {}", host, message);
                if search.test(&search_string) {
                    let log_entry = Log{
                        id: row.get(0)?,
                        message: message,
                        host: host,
                        time: row.get(3)?,
                    };
                    results.push(log_entry);
                }
                else{
                    //println!("Event did not match search: {}", search_string);
                }
            }
        }

        Ok(results)
    }
}

const MAX_WRITE_PER_SECOND_PER_THREAD: usize = 3000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WriteTicket{
    days: u32,
    hours: u32,
    minutes: u32,
    machine_id: u32,
    node_id: u32,
}

pub struct ShardedMinute{
    tickets: HashSet<WriteTicket>,
    machine_id: u32,
    data_directory: String,
}

impl ShardedMinute{
    pub fn new(machine_id: u32, data_directory: String) -> ShardedMinute {
        /*
            Note: we're storing WriteTickets in RAM, here, which means that if the server crashes, there's a good chance we'll
                lose tickets and a bunch of minutes will be left unsealed.
            This is a problem, but it's not a problem we need to solve right now.
            It's a problem for _future curtis_.
         */
        ShardedMinute{
            tickets: HashSet::default(),
            machine_id: machine_id,
            data_directory,
        }
    }

    pub fn write(&mut self, data: Vec<crate::WritableEvent>) -> Result<()> {
        let n_threads = (data.len() / MAX_WRITE_PER_SECOND_PER_THREAD as usize) + 1;
        let mut threads = Vec::new();
        let mut data = data.clone();

        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as u32;
        let day = timestamp / 86400;
        let hour = (timestamp % 86400) / 3600;
        let minute = (timestamp % 3600) / 60;

        for n in 0..n_threads {
            // grab the first MAX_WRITE_PER_SECOND_PER_THREAD events
            let split_data: Vec<crate::WritableEvent>;
            if data.len() < MAX_WRITE_PER_SECOND_PER_THREAD {
                split_data = data.clone();
                data.clear();
            } else {
                let split_point = std::cmp::max(data.len()-MAX_WRITE_PER_SECOND_PER_THREAD, 0);
                split_data = data.split_off(split_point);
            }
            self.tickets.insert(WriteTicket{
                days: day,
                hours: hour,
                minutes: minute,
                machine_id: self.machine_id,
                node_id: n as u32,
            });
            let data_directory = self.data_directory.clone();
            let unique_id = format!("{}-{}", self.machine_id, n);
            let thread = std::thread::spawn(move || {
                // each writer lives on its own thread
                let mut minute = Minute::new(
                    day, hour, minute, &unique_id, &data_directory).unwrap();

                if split_data.len() > 0 {
                    match minute.write_second(split_data){
                        Ok(_) => (),
                        Err(e) => println!("Error writing to minute: {}", e)
                    }
                }
            });
            threads.push(thread);
        }
        for thread in threads {
            thread.join().unwrap();
        }

        self.seal()?;

        Ok(())
    }

    ///
    /// BAABY I COMPARE YOU TO A KISS FROM A ROSE ON THE GREY
    /// OOOH THE MORE I GET OF YOU THE STRANGER IT FEELS YEAH
    /// NOW THAT YOUR ROSE IS IN BLOOM
    /// A LIGHT HITS THE GLOOM ON THE GREY
    /// (seal any minutes that are in the past: we will never write to them again)
    ///
    pub fn seal(&mut self) -> Result<()> {
        for node in &self.tickets {
            let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as u32;
            let day = timestamp / 86400;
            let hour = (timestamp % 86400) / 3600;
            let minute = (timestamp % 3600) / 60;
            if !(node.days == day && node.hours == hour && node.minutes == minute) {
                // we should only seal the minute if it's not the current minute
                let unique_id = format!("{}-{}", node.machine_id, node.node_id);
                let mut minute = Minute::new(
                    node.days,
                    node.hours,
                    node.minutes,
                    &unique_id,
                    &self.data_directory).unwrap();
                minute.seal()?;
            }
        }
        Ok(())
    }

    ///
    /// Normally we would seal the minute when it's time to seal the minute, but this forces every minute that the
    /// ShardedMinute has a ticket for to be sealed.
    ///  (it's only intended to be used for testing)
    ///
    #[allow(dead_code)]
    pub fn force_seal(&mut self) -> Result<()> {
        for node in &self.tickets {
            let unique_id = format!("{}-{}", node.machine_id, node.node_id);
            let mut minute = Minute::new(
                node.days,
                node.hours,
                node.minutes,
                &unique_id,
                &self.data_directory).unwrap();
            minute.seal()?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub struct TestData{
    lines: Vec<String>,
    i: usize,
}

#[allow(dead_code)]
impl TestData{
    pub fn new() -> Self {
        // open a file and read it into memory
        // split it into lines
        let contents = fs::read_to_string("../test-log-generator/sample.log").unwrap();
        let lines = contents.split("\n").map(|x| x.to_string()).collect();

        TestData{lines, i: 0}
    }

    pub fn next(&mut self) -> String {
        let line = self.lines[self.i].clone();
        self.i += 1;
        if self.i >= self.lines.len() {
            self.i = 0;
        }
        line
    }
}

#[allow(dead_code)]
pub fn generate_test_data(data: &mut TestData) -> crate::WritableEvent {
    crate::WritableEvent{
        event: data.next(),
        time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64,
        host: "localhost".to_string()
    }
}

#[allow(dead_code)]
fn generate_needle() -> crate::WritableEvent {
    crate::WritableEvent{
        event: "haystack haystack haystack haystack haystack haystack needle haystack haystack haystack haystack".to_string(),
        time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64,
        host: "localhost".to_string()
    }
}

#[allow(dead_code)]
fn generate_haystack() -> crate::WritableEvent {
    crate::WritableEvent{
        event: "haystack haystack haystack haystack haystack haystack haystack haystack haystack".to_string(),
        time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64,
        host: "localhost".to_string()
    }
}


#[test]
fn test_explode() -> Result<()> {
    let mut fragments = HashSet::default();
    Minute::explode(&mut fragments, &"hello world".to_string());

    assert!(fragments.contains("hel"));
    assert!(fragments.contains("ell"));
    assert!(fragments.contains("llo"));
    assert!(fragments.contains("wor"));
    assert!(fragments.contains("orl"));
    assert!(fragments.contains("rld"));
    Ok(())
}

#[test]
fn test_explode_speed() -> Result<()> {
    let mut fragments = HashSet::default();
    // start a timer
    let start = SystemTime::now();
    for _ in 0..10000 {
        Minute::explode(&mut fragments, &"prod-api-blue-gusher-37l master-build-2024-03-14-pogo-q-humslash notice: r=ggsc8rn0 - m=GET u=/api/1/worlds/wrld_5ef1f09c-a4dc-4fef-8cc1-45d9b82dbe00?apiKey=JlE5Jldo5Jibnk5O5hTx6XVqsJu4WJ26&organization=vrchat ip=240f:77:1cc0:1:29ff:87db:78e8:274f mac=e84e9e5dcad93e0a470b06dfeb1d5bd780965fac country=JP asn=2516 ja3=00000000000000000000000000000000 uA=VRC.Core.BestHTTP-Y platform=standalonewindows gsv=Release_1343 store=steam clientVersion=2024.1.1p2-1407--Release unityVersion=2022.3.6f1-DWR autok=b44d782088b32903 uId=usr_18698e31-bd1a-4aa6-b1a0-44cf9c51ab00 2fa=N lv=44 f=78 ms=4 s=200 route=/api/1/worlds/:id - TIME_OK".to_string());
    }

    let elapsed = start.elapsed().unwrap();
    let elapsed_s = elapsed.as_secs() as i128;
    assert!(elapsed_s < 10);
    Ok(())
}

#[test]
fn test_explode_unicode() -> Result<()> {
    let unicode = "dN=\u{30c1}\u{30e7}\u{30b3}\u{7f8e}\u{5473}\u{3044}".to_string();
    let mut fragments = HashSet::default();
    Minute::explode(&mut fragments, &unicode);

    Ok(())
}

#[allow(dead_code)]
pub fn test_data_directory(test_name: &str) -> String {
    let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as u32;
    format!("./test_data/test_{}_{}", test_name, timestamp)
}

#[test]
fn test_minute_writer() -> Result<()> {
    let mut minute = Minute::new(
        2,
        4,
        6,
        "quick",
        &test_data_directory("minute_writer"))?;

    let mut test_data_source = TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    minute.write_second(test_data)?;

    minute.seal()?;

    Ok(())
}

#[test]
fn test_minute_search() -> Result<()> {
    let mut minute = Minute::new(
        2,
        4,
        6,
        "search",
        &test_data_directory("minute_search"))?;

    let mut test_data_source = TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    minute.write_second(test_data)?;

    minute.seal()?;

    let searchterm = "not writable";

    let results = minute.search(&crate::search_token::Search::new(searchterm))?;
    assert!(results.len() > 0);
    assert!(results[0].message.contains(searchterm));
    assert!(results.len() < 1000);

    let searchterm = "presence";

    let results = minute.search(&crate::search_token::Search::new(searchterm))?;
    assert!(results.len() > 0);
    assert!(results[0].message.contains(searchterm));
    assert!(results.len() < 1000);

    let searchterm = "presence !homer";

    let results = minute.search(&crate::search_token::Search::new(searchterm))?;
    assert!(results.len() > 0);
    assert!(results[0].message.contains("presence"));
    assert!(!results[0].message.contains("homer"));
    assert!(results.len() < 1000);


    Ok(())
}

#[test]
fn test_generated_bloom() -> Result<()> {
    let mut minute = Minute::new(
        1,
        2,
        3,
        "bloom",
        &test_data_directory("generated_bloom"))?;

    for _ in 0..5{
        let mut test_data = Vec::new();
        for i in 0..1000 {
            if i % 384 == 0 {
                let data = generate_needle();
                test_data.push(data);
            } else {
                let data = generate_haystack();
                test_data.push(data);
            }
        }
        minute.write_second(test_data)?;
    }
    minute.seal()?;

    let bloom = minute.get_bloom_filter()?;
    assert!(bloom.contains("hay"));
    assert!(bloom.contains("ays"));
    assert!(bloom.contains("yst"));
    assert!(bloom.contains("sta"));
    assert!(bloom.contains("tac"));
    assert!(bloom.contains("ack"));

    assert!(bloom.contains("nee"));
    assert!(bloom.contains("eed"));
    assert!(bloom.contains("edl"));
    assert!(bloom.contains("dle"));

    Ok(())
}


#[test]
fn test_sharded_minute() -> Result<()> {
    let mut minute = ShardedMinute::new(
        1,
        test_data_directory("sharded_minute").to_string());
    let mut test_data_source = TestData::new();

    // start a timer
    let start = SystemTime::now();

    // 60 times, write a second of data
    let mut count = 0;
    let mut bytes = 0;
    for _ in 0..60 {
        let mut test_data = Vec::new();
        for _ in 0..1000 {
            let data = generate_test_data(&mut test_data_source);
            count += 1;
            bytes += data.get_size_in_bytes();
            test_data.push(data);
        }
        minute.write(test_data)?;
    }

    // stop the timer
    let elapsed = start.elapsed().unwrap();
    //let elapsed_us = elapsed.as_micros() as i128;
    let elapsed_ms = elapsed.as_millis() as i128;
    //let elapsed_s = elapsed.as_secs() as i128;
    //println!("Wrote {} events ({} bytes, {}/sec) in {} us, {} ms, {} s", count, bytes, bytes/60, elapsed_us, elapsed_ms, elapsed_s);
    assert!(elapsed_ms < 60000);

    let start = SystemTime::now();
    // force seal the minute
    minute.force_seal()?;
    let elapsed = start.elapsed().unwrap();
    let elapsed_ms = elapsed.as_millis() as i128;
    //println!("Sealed in {} ms", elapsed_ms);
    assert!(elapsed_ms < 10000);

    Ok(())
}