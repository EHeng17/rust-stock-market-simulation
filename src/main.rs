mod broker;
mod client;
mod stock_exchange;

use std::thread;
use client::client;
use broker::broker;
use stock_exchange::stock_exchange;

fn main() {

    // Bursa Malaysia
    thread::spawn(move || {
        if let Err(err) = stock_exchange() {
            println!("Error occurred in Stock Exchange: {:?}", err);
        }
    });

    // Broker 1
    thread::spawn(move || {
        if let Err(err) = broker("1".to_string(), 254, 47, 12) {
            println!("Error occurred in Broker 1: {:?}", err);
        }
    });

    // Broker 2
    thread::spawn(move || {
        if let Err(err) = broker("2".to_string(), 47, 12, 254) {
            println!("Error occurred in Broker 2: {:?}", err);
        }
    });

    // Client 1
    thread::spawn(move || {
        if let Err(err) = client("1".to_string(), "1".to_string()) {
            println!("Error occurred in Client 1: {:?}", err);
        }
    });

    // Client 2
    thread::spawn(move || {
        if let Err(err) = client("2".to_string(), "2".to_string()) {
            println!("Error occurred in Client 2: {:?}", err);
        }
    });

    loop {}
}
