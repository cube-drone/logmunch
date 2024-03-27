#[macro_use] extern crate rocket;
use std::sync::Arc;
use rocket::data::Data;
use rocket::data::ToByteUnit;
use rocket::State;
use rocket::serde::json::Json;
use serde::Deserialize;
use crossbeam::channel::unbounded;
use crossbeam::channel::{Sender, Receiver};
use rocket::tokio;
use anyhow::Result;

mod minute;
mod minute_id;
mod minute_db;
mod search_token;

mod file_list;

/*
POST /services/collector/event/1.0 {}
HEADERS:
{
  host: 'host.docker.internal:9283',
  'user-agent': 'Go-http-client/1.1',
  'content-length': '1335',
  authorization: 'Splunk SPLUNK-TOKEN-GOES-HERE',
  'accept-encoding': 'gzip'
}
BODY:
[
  {
    event: 'SPLUNK-TAG HAMS_AHOY2=SWINEFLESH 2023-11-10T14:55:41.810865+00:00 marquee 1349ca097c74 700331 -  GET /test 200 2 - 0.158 ms',
    time: '1710562887.366663',
    host: 'docker-desktop'
  },
  {
    event: 'SPLUNK-TAG HAMS_AHOY2=SWINEFLESH 2023-11-10T14:55:41.810988+00:00 marquee orchestr8 - -  Success: http://localhost:12249 responded!',
    time: '1710562888.368497',
    host: 'docker-desktop'
  },
  {
    event: 'SPLUNK-TAG HAMS_AHOY2=SWINEFLESH 2023-11-10T14:55:42.012827+00:00 marquee orchestr8 - -  all deployments for github-info are healthy',
    time: '1710562889.369968',
    host: 'docker-desktop'
  },
  {
    event: 'SPLUNK-TAG HAMS_AHOY2=SWINEFLESH 2023-11-10T14:55:42.262540+00:00 girlboss 09c01c523eef 300704 -  212.102.46.118 - - [10/Nov/2023:14:55:42 +0000] "POST /presence/update HTTP/1.1" 403 99 "https://marquee.click/t/homer-man-x/2187" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"',
    time: '1710562890.371295',
    host: 'docker-desktop'
  },
  {
    event: 'SPLUNK-TAG HAMS_AHOY2=SWINEFLESH 2023-11-10T14:55:43.270854+00:00 girlboss 09c01c523eef 300704 -  212.102.46.118 - - [10/Nov/2023:14:55:43 +0000] "POST /presence/update HTTP/1.1" 403 99 "https://marquee.click/t/homer-man-x/2187" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"',
    time: '1710562891.372572',
    host: 'docker-desktop'
  }
]

*/

#[derive(Deserialize)]
struct InputEvent{
    event: String,
    time: String,
    host: String
}

impl InputEvent{
    pub fn to_writable_event(&self) -> WritableEvent{
        let time_microseconds = (self.time.parse::<f64>().unwrap() * 1000000.0) as i64;
        WritableEvent{
            event: self.event.clone(),
            time: time_microseconds,
            host: self.host.clone()
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
struct WritableEvent{
    event: String,
    time: i64,
    host: String
}

impl WritableEvent{
    pub fn get_size_in_bytes(&self) -> usize {
        self.event.len() + self.host.len() + 8
    }
}


#[options("/services/collector/event/<version>")]
fn ingest_options_endpoint(version: f32) -> &'static str {
    let _version = version;
    "OK"
}

async fn do_something(services: &State<Services>, row: &str){
    // do something with row
    let event = serde_json::from_str::<InputEvent>(row).unwrap();

    services.sender.send(event.to_writable_event()).unwrap();
}

#[post("/services/collector/event/<version>", data="<data>")]
async fn ingest_endpoint(services: &State<Services>, data: Data<'_>, version: f32) -> &'static str {

    let stream = data.open(10.megabytes());
    let str = stream.into_string().await;
    let _version = version;

    let mut charbuffer: Vec<char> = Vec::new();
    let mut in_quotes = false;
    let mut cancel = false;

    for character in str.unwrap().into_inner().chars() {
        charbuffer.push(character);

        if character == '"' && !cancel{
            in_quotes = !in_quotes;
            cancel = false;
        }
        else if character == '}' && !cancel && !in_quotes{
            let row: String = charbuffer.into_iter().collect();
            do_something(&services, &row).await;
            charbuffer = Vec::new();
        }
        else if character == '\\'{
            cancel = !cancel;
        }
        else{
            cancel = false;
        }
    }

    "OK"
}

#[get("/search/<search>")]
async fn search_endpoint(services: &State<Services>, search: &str) -> Json<Vec<crate::minute::Log>> {
    let search = search_token::Search::new(&search);

    let results = match services.minute_db.search_async(search).await{
        Ok(results) => results,
        Err(err) => {
            println!("Error searching: {:?}", err);
            Vec::new()
        }
    };

    Json(results)
}

#[derive(Clone)]
pub struct Services{
    sender: Arc<Sender<WritableEvent>>,
    receiver: Arc<Receiver<WritableEvent>>,
    minute_db: Arc<minute_db::MinuteDB>,
}

const ESTIMATED_MINUTE_BLOOM_SIZE_BYTES: u64 = 1500000;
const ESTIMATED_MINUTE_DISK_SIZE_BYTES: u64 = 100000000;

#[launch]
async fn rocket() -> _ {

    let (sender, receiver) = unbounded::<WritableEvent>();

    // TODO: these things should be configurable env vars
    // mathin' it out: 1 day (1440 minutes) should occupy about 270MB of RAM, and .... 144GB of disk
    //  this is based on the assumption that each minute occupies 1.5MB of RAM and 100MB of disk
    //  and that our ShardedMinuteWriter isn't writing more than one Minute object per minute
    //      (which it starts to do past 3000 lines/s or 180000 lines/m)
    let minute_db_gigabytes_string = std::env::var("MINUTE_DB_RAM_GB").unwrap_or("1.8".to_string());
    let minute_db_disk_gigabytes_string = std::env::var("MINUTE_DB_DISK_GB").unwrap_or("20".to_string());
    let minute_db_bytes = (minute_db_gigabytes_string.parse::<f64>().unwrap() * 1024.0 * 1024.0 * 1024.0) as u64;
    let minute_db_disk_bytes = (minute_db_disk_gigabytes_string.parse::<f64>().unwrap() * 1024.0 * 1024.0 * 1024.0) as u64;

    let machine_id = std::env::var("MACHINE_ID").unwrap_or("1".to_string()).parse::<u32>().unwrap();

    // DATA_DIRECTORY is where we store the minute files
    let data_directory = std::env::var("DATA_DIRECTORY").unwrap_or("./data/".to_string());
    let minute_data_directory = format!("{}/minutes", data_directory);
    // TODO: make sure the directory exists
    // TODO: classic_data_directory for storing logs ... in a regular file!
    let minute_db_n_max_minutes_for_ram = minute_db_bytes / ESTIMATED_MINUTE_BLOOM_SIZE_BYTES;
    let minute_db_n_max_minutes_for_disk = minute_db_disk_bytes / ESTIMATED_MINUTE_DISK_SIZE_BYTES;
    let minute_db_n_minutes = std::cmp::min(minute_db_n_max_minutes_for_ram, minute_db_n_max_minutes_for_disk);

    let max_write_threads = std::env::var("MAX_WRITE_THREADS").unwrap_or("2".to_string()).parse::<u32>().unwrap();

    if minute_db_n_minutes < 5 {
        panic!("Not enough memory or disk space to run this program!");
    }
    else if minute_db_n_minutes == minute_db_n_max_minutes_for_ram {
        println!("Booting with {} minutes in memory: increase minute cache length by increasing RAM", minute_db_n_minutes);
    }
    else if minute_db_n_minutes == minute_db_n_max_minutes_for_disk {
        println!("Booting with {} minutes in memory: increase minute cache length by adding disk space", minute_db_n_minutes);
    }

    let services = Services{
        sender: Arc::new(sender),
        receiver: Arc::new(receiver),
        minute_db: Arc::new(minute_db::MinuteDB::new(minute_db_n_minutes, minute_data_directory.to_string())),
    };

    let mut app = rocket::build();
    app = app.manage(services.clone());
    app = app.mount("/", routes![ingest_options_endpoint, ingest_endpoint, search_endpoint]);

    tokio::task::spawn_blocking(move || {
        // this is the write thread and it's just gonna spin forever
        let mut minute_writer = minute::ShardedMinute::new(machine_id, minute_data_directory.to_string(), max_write_threads);

        minute_writer.write_loop(services.receiver.clone());
    });

    tokio::task::spawn_blocking(move || {
        let minute_reader = services.minute_db.clone();

        minute_reader.read_loop();
    });

    app
}
