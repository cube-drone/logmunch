use std::time::SystemTime;
use std::fs;
use anyhow::Result;

use fxhash::FxHashSet as HashSet;
use growable_bloom_filter::GrowableBloom;
use postcard;

use rusqlite::{Connection as SqlConnection, DatabaseName, params, Transaction};

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

// Minute isn't intended to be passed around between threads, so it's not Sync, or Send, or nothin'
pub struct Minute{
    connection: SqlConnection,
}

const CREATE_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS log (
    id INTEGER PRIMARY KEY,
    log TEXT NOT NULL,
    host TEXT NOT NULL,
    host_time INTEGER NOT NULL
)"#;

const INDEX_TIME: &str = r#"CREATE INDEX IF NOT EXISTS log_host_time ON log (host_time)"#;
const INDEX_HOST: &str = r#"CREATE INDEX IF NOT EXISTS log_host ON log (host)"#;

const INSERT_LOG: &str = r#"INSERT INTO log (id, log, host, host_time) VALUES (?, ?, ?, ?)"#;

const CREATE_SEARCH_FRAGMENTS: &str = r#"CREATE TABLE IF NOT EXISTS search_fragments (
    id INTEGER PRIMARY KEY,
    fragment TEXT,
    min_log_id INTEGER,
    max_log_id INTEGER
)"#;

const INDEX_FRAGMENT: &str = r#"CREATE INDEX IF NOT EXISTS search_fragments_fragment ON search_fragments (fragment)"#;

const INSERT_FRAGMENT: &str = r#"INSERT INTO search_fragments (id, fragment, min_log_id, max_log_id) VALUES (?, ?, ?, ?)"#;

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

        execute_and_eat_already_exists_errors(&connection, CREATE_TABLE)?;
        execute_and_eat_already_exists_errors(&connection, CREATE_SEARCH_FRAGMENTS)?;
        execute_and_eat_already_exists_errors(&connection, CREATE_BLOOM)?;

        Ok(Minute{
            connection,
        })
    }

    pub fn explode(fragments: &mut HashSet<String>, data: &String){
        // this hashset contains every word in the string
        // it also contains every 3-letter fragment of every word
        for word in data.split_whitespace() {
            fragments.insert(word.to_string().to_lowercase());

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
            for token in word.split("="){
                fragments.insert(token.to_string());
            }
            for token in word.split("-"){
                fragments.insert(token.to_string());
            }
            for token in word.split(":"){
                fragments.insert(token.to_string());
            }
            for token in word.split("/"){
                fragments.insert(token.to_string());
            }
        }
    }

    fn write_events_to_transaction(tx: &Transaction, data: Vec<crate::WritableEvent>) -> Result<()> {
        let mut statement = tx.prepare_cached(INSERT_LOG)?;
        let mut fragment_statement = tx.prepare_cached(INSERT_FRAGMENT)?;
        let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as i64;
        let mut sequence = 0;
        let first_id = (timestamp * 1000000) + 0 as i64;
        let mut last_id = 0;
        let mut fragments: HashSet<String> = HashSet::default();
        // Lock the connection
        for event in data {
            //self.bytes += event.get_size_in_bytes() as u32;
            Minute::explode(&mut fragments, &event.event);
            fragments.insert(event.host.clone());

            last_id = (timestamp * 1000000) + sequence as i64;
            sequence += 1;

            statement.execute(params![last_id, event.event, event.host, event.time])?;
        }
        // remove the empty string, nobody wants that
        //fragments.remove("");
        for fragment in fragments {
            sequence += 1;
            let id = (timestamp * 1000000) + sequence as i64;
            fragment_statement.execute(params![id, fragment, first_id, last_id])?;
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
        self.connection.execute(INDEX_FRAGMENT, [])?;

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

    pub fn search(&self, search_string: &crate::search_token::SearchTree) -> Result<Vec<String>> {
        //
        // We can't get to the search function without having first verified that the minute is sealed
        // the bloom filter is available, and most of all: it said that there's (probably) the search string in the minute
        //

        // for each fragment in the search string, we need to check the search_fragments table to determine if it
        // can be found there?

        Ok(Vec::new())
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
    /// (mostly for testing, I think?)
    /// seal every minute
    ///
    pub fn force_seal(&mut self) -> Result<()> {
        for node in &self.tickets {
            let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as u32;
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


struct TestData{
    lines: Vec<String>,
    i: usize,
}

impl TestData{
    fn new() -> Self {
        // open a file and read it into memory
        // split it into lines
        let contents = fs::read_to_string("../test-log-generator/sample.log").unwrap();
        let lines = contents.split("\n").map(|x| x.to_string()).collect();

        TestData{lines, i: 0}
    }

    fn next(&mut self) -> String {
        let line = self.lines[self.i].clone();
        self.i += 1;
        if self.i >= self.lines.len() {
            self.i = 0;
        }
        line
    }
}

fn generate_test_data(data: &mut TestData) -> crate::WritableEvent {
    crate::WritableEvent{
        event: data.next(),
        time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64,
        host: "localhost".to_string()
    }
}

fn generate_needle() -> crate::WritableEvent {
    crate::WritableEvent{
        event: "haystack haystack haystack haystack haystack haystack needle haystack haystack haystack haystack".to_string(),
        time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64,
        host: "localhost".to_string()
    }
}
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

    assert!(fragments.contains("hello"));
    assert!(fragments.contains("world"));
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

#[test]
fn test_minute_writer() -> Result<()> {
    let mut minute = Minute::new(
        1,
        2,
        3,
        "quick",
        &"./test_data")?;

    let mut test_data_source = TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    minute.write_second(test_data)?;

    Ok(())
}

#[test]
fn test_minute_reader() -> Result<()> {
    let mut minute = Minute::new(
        1,
        2,
        3,
        "toast",
        &"./test_data")?;

    for i in 0..5{
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

    let reader = Minute::new(
        1,
        2,
        3,
        "toast",
        &"./test_data")?;

    let bloom = reader.get_bloom_filter()?;
    assert!(bloom.contains("haystack"));
    assert!(bloom.contains("hay"));
    assert!(bloom.contains("needle"));
    assert!(bloom.contains("nee"));
    assert!(bloom.contains("eed"));
    assert!(bloom.contains("edl"));
    assert!(bloom.contains("dle"));

    Ok(())
}


#[test]
fn test_minute() -> Result<()> {
    let mut minute = ShardedMinute::new(
        1,
        "./test_data".to_string());
    let mut test_data_source = TestData::new();

    // start a timer
    let start = SystemTime::now();

    // 60 times, write a second of data
    let mut count = 0;
    let mut bytes = 0;
    for _ in 0..60 {
        let mut test_data = Vec::new();
        for _ in 0..5000 {
            let data = generate_test_data(&mut test_data_source);
            count += 1;
            bytes += data.get_size_in_bytes();
            test_data.push(data);
        }
        minute.write(test_data)?;
    }

    // stop the timer
    let elapsed = start.elapsed().unwrap();
    let elapsed_us = elapsed.as_micros() as i128;
    let elapsed_ms = elapsed.as_millis() as i128;
    let elapsed_s = elapsed.as_secs() as i128;
    println!("Wrote {} events ({} bytes, {}/sec) in {} us, {} ms, {} s", count, bytes, bytes/60, elapsed_us, elapsed_ms, elapsed_s);

    let start = SystemTime::now();
    // force seal the minute
    minute.force_seal()?;
    let elapsed = start.elapsed().unwrap();
    let elapsed_ms = elapsed.as_millis() as i128;
    println!("Sealed in {} ms", elapsed_ms);

    Ok(())
}