
extern crate paho_mqtt as mqtt;

use std::{process,
          thread,
          time::Duration,
          time::UNIX_EPOCH,
          str,
          result::Result,
};

extern crate chrono;
use chrono::prelude::DateTime;
use chrono::Utc;
use postgres::Error;
use mqtt_temperatures1::structs::t_sensor::TSensor;

use postgres::{Client, NoTls};


// postgres-# \d temperatures_tarts 
//                 Table "public.temperatures_tarts"
//  Column  |         Type         | Collation | Nullable | Default 
// ---------+----------------------+-----------+----------+---------
//  time    | bigint               |           | not null | 
//  sensor  | character varying(8) |           |          | 
//  rssi    | integer              |           |          | 
//  battery | real                 |           |          | 
//  cvalue  | real                 |           |          | 
//  fvalue  | real                 |           |          | 
//  is_real | boolean              |           |          | 


const DFLT_TOPICS:&[&str] = &["sensor/TEMPERATURE/#"];
// The qos list that match topics above.
const DFLT_QOS:&[i32] = &[0];


fn subscribe_topics(cli: &mqtt::Client) {
    if let Err(e) = cli.subscribe_many(DFLT_TOPICS, DFLT_QOS) {
        println!("Error subscribes topics: {:?}", e);
        process::exit(1);
    }
}

fn try_reconnect(cli: &mqtt::Client) -> bool
{
    println!("Connection lost. Waiting to retry connection");
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(5000));
        if cli.reconnect().is_ok() {
            println!("Successfully reconnected");
            return true;
        }
    }
    println!("Unable to reconnect after several attempts.");
    false
}


fn main() -> Result<(), Error> {
    // Create a client & define connect options
    let mut cli = mqtt::Client::new("tcp://rasp4-1:1883").unwrap_or_else(|err| {
        println!("Error creating the client: {}", err);
        process::exit(1);
    });
     
    let conn_opts = mqtt::ConnectOptions::new();
    // Initialize the consumer before connecting.
    let rx = cli.start_consuming();

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(conn_opts) {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    
    // initialize the dbase access
    let dsn = "postgresql://postgres:pgres2021@rasp4-1/sensors";
    let mut client = Client::connect(dsn, NoTls)?;

    // Subscribe topics.
    subscribe_topics(&cli);

    println!("Processing requests...");

    for msg in rx.iter() {
        if let Some(msg) = msg {
            println!("{}", msg.topic());

            let s = match str::from_utf8(msg.payload()){
                Ok(v) => v,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            println!("{}", s);
            let v1: TSensor = serde_json::from_str(&s).unwrap();
            println!("sensor = {}", v1.sensor);
            let v_sensor = v1.sensor;
            let v_rssi: i32 = v1.rssi;
            let v_battery: f32 = v1.battery;
            let celcius: f32 = v1.value / 10.0;
            println!("celcius = {}", celcius);
            let fahrenheit: f32 = (9.0 / 5.0 * celcius) + 32.0; 
            println!("fahrenheit = {}", fahrenheit);
            let its_real = true;

            // convert the unix timestamp
            let d = UNIX_EPOCH + Duration::from_secs(v1.time);
            // Create DateTime from SystemTime
            let datetime = DateTime::<Utc>::from(d);

            // Write to the dbase
            let stmt = "insert into tarts_temperatures (time, sensor, rssi, battery, cvalue, fvalue, is_real) values ($1, $2, $3, $4, $5, $6, $7)";
            match client.execute(stmt, &[&datetime, &v_sensor, &v_rssi, &v_battery, &celcius, &fahrenheit, &its_real]){
                Err(error) => println!("Error in dbase write {}", error.to_string()),
                Ok(_) => (),
            };
        }
        else if !cli.is_connected() {
            if try_reconnect(&cli) {
                println!("Resubscribe topics...");
                subscribe_topics(&cli);
            } else {
                break;
            }
        }
    }


    println!("All done now!");
    Ok(())
}
