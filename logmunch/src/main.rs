#[macro_use] extern crate rocket;
use std::sync::Arc;
use rocket::data::Data;
use rocket::data::ToByteUnit;
use rocket::State;
use serde::{Deserialize, Serialize};
use crossbeam::channel::unbounded;
use crossbeam::channel::{Sender, Receiver};
use rocket::tokio;

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

#[derive(Clone, PartialEq, Debug)]
struct WritableEvent{
    event: String,
    time: i64,
    host: String
}


#[options("/services/collector/event/<version>")]
fn ingest_options(version: f32) -> &'static str {
    let _version = version;
    "OK"
}

async fn do_something(services: &State<Services>, row: &str){
    // do something with row
    println!("{}", row);
    let event = serde_json::from_str::<InputEvent>(row).unwrap();
    let time_microseconds = (event.time.parse::<f64>().unwrap() * 1000000.0) as i64;
    let writable_event = WritableEvent{
        event: event.event,
        time: time_microseconds,
        host: event.host
    };
    services.sender.send(writable_event).unwrap();
}

#[post("/services/collector/event/<version>", data="<data>")]
async fn ingest(services: &State<Services>, data: Data<'_>, version: f32) -> &'static str {

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

#[derive(Clone)]
pub struct Services{
    sender: Arc<Sender<WritableEvent>>,
    receiver: Arc<Receiver<WritableEvent>>
}

#[launch]
async fn rocket() -> _ {

    let (sender, receiver) = unbounded::<WritableEvent>();

    let services = Services{
        sender: Arc::new(sender),
        receiver: Arc::new(receiver)
    };

    let mut app = rocket::build();
    app = app.manage(services.clone());
    app = app.mount("/", routes![ingest_options, ingest]);

    tokio::spawn(async move {
        loop {
            let event = services.receiver.recv().unwrap();
            println!("{:?}", event);
        }
    });

    app
}
