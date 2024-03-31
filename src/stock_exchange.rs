extern crate rand;
extern crate colored;
extern crate prettytable;
extern crate crossbeam_channel;
extern crate scheduled_thread_pool;

use rand::Rng;
use colored::Colorize;
use std::{thread, vec};
use std::time::Duration;
use crossbeam_channel::unbounded;
use serde::{Serialize, Deserialize};
use std::sync::{mpsc::channel, Arc, Mutex};
use scheduled_thread_pool::ScheduledThreadPool;
use amiquip::{Exchange, AmqpProperties, ConsumerMessage, Connection, ExchangeDeclareOptions, ExchangeType, Publish, Result, ConsumerOptions, QueueDeclareOptions};

use crate::broker::BuySellStockInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stock {
    pub name: String,
    pub symbol: String,
    pub value: f32,
    pub stock_direction: String,
    pub volatility: f32
}

pub fn stock_exchange() -> Result<()> {

    println!("{}", "Bursa Malaysia has started !!!\n".green().bold());

    // -------------------- RabbitMQ Broadcast (Publisher/Subscriber) --------------------

    // Open connection (RabbitMQ).
    let connection = Arc::new(Mutex::new(Connection::insecure_open("amqp://guest:guest@localhost:5672")?));
    let connection_clone = connection.clone();

    // Open a channel - None says let the library choose the channel ID.
    let channel_rabbit_mq = connection.lock().unwrap().open_channel(None)?;

    // Direct Exchange from broker to stock exchange
    let direct_exchange = Exchange::direct(&channel_rabbit_mq);

    // Queue from broker to stock exchange
    let broker_queue = channel_rabbit_mq.queue_declare("buy_sell_stock_queue", QueueDeclareOptions::default())?;

    // Clear queue
    let _ = broker_queue.purge(); 

    // Start a consumer.
    let broker_consumer = broker_queue.consume(ConsumerOptions::default())?;

    // -------------------------------------------------------------

    // Initial broadcast to brokers (Simulating opening of stock exchange)
    let mut initial = true;

    // Low thread count to prevent heavy use of resources
    let stock_selector_pool = ScheduledThreadPool::new(2);
    let stock_updater_pool = ScheduledThreadPool::new(2);

    let(select_sender, select_receiver) = unbounded(); // Crossbeam for Stock Selector
    let (broker_sender, broker_receiver) = channel(); // Channel to Send to Broker

    let stocks: Vec<Arc<Mutex<Stock>>> = vec![
        Arc::new(Mutex::new(Stock {
            name: "Hong Seng Consolidated Bhd".to_string(),
            symbol: "HONGSENG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Lambo Group Bhd".to_string(),
            symbol: "LAMBO".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.33
        })),
        Arc::new(Mutex::new(Stock {
            name: "MMAG Holdings Bhd".to_string(),
            symbol: "MMAG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.17
        })),
        Arc::new(Mutex::new(Stock {
            name: "My E.G. Services Berhad".to_string(),
            symbol: "MYEG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.02
        })),
        Arc::new(Mutex::new(Stock {
            name: "NetX Holdings Bhd".to_string(),
            symbol: "NETX".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.45
        })),
        Arc::new(Mutex::new(Stock {
            name: "Asdion Bhd".to_string(),
            symbol: "ASDION".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.16
        })),
        Arc::new(Mutex::new(Stock {
            name: "CTOS Digital Bhd".to_string(),
            symbol: "CTOS".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.5
        })),
        Arc::new(Mutex::new(Stock {
            name: "Cloudpoint Technology Bhd".to_string(),
            symbol: "CLOUDPT".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.6
        })),
        Arc::new(Mutex::new(Stock {
            name: "Eduspec Holdings Bhd".to_string(),
            symbol: "EDUSPEC".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.53
        })),
        Arc::new(Mutex::new(Stock {
            name: "HeiTech Padu Bhd".to_string(),
            symbol: "HTPADU".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.53
        })),
        // ------------------------ 10 Stocks ------------------------
        Arc::new(Mutex::new(Stock {
            name: "Southern Score Builders Berhad".to_string(),
            symbol: "SSB8".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.9
        })),
        Arc::new(Mutex::new(Stock {
            name: "Bina Puri Holdings Bhd".to_string(),
            symbol: "BPURI".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "Eversendai Corporation Bhd".to_string(),
            symbol: "SENDAI".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.6
        })),
        Arc::new(Mutex::new(Stock {
            name: "TCS Group Holdings Bhd".to_string(),
            symbol: "TCS".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.28
        })),
        Arc::new(Mutex::new(Stock {
            name: "Jati Tinggi Group Bhd".to_string(),
            symbol: "JTGROUP".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.52
        })),
        Arc::new(Mutex::new(Stock {
            name: "Widad Group Bhd".to_string(),
            symbol: "WIDAD".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.27
        })),
        Arc::new(Mutex::new(Stock {
            name: "Gamuda Bhd".to_string(),
            symbol: "GAMUDA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Petronas Gas Bhd".to_string(),
            symbol: "PETGAS".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Muhibbah Engineering (M) Bhd".to_string(),
            symbol: "MUHIBAH".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.5
        })),
        Arc::new(Mutex::new(Stock {
            name: "Econpile Holdings Bhd".to_string(),
            symbol: "ECONBHD".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.4
        })),
        // ------------------------ 10 Stocks ------------------------
        Arc::new(Mutex::new(Stock {
            name: "Top Glove Corporation Bhd".to_string(),
            symbol: "TOPGLOV".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.4
        })),
        Arc::new(Mutex::new(Stock {
            name: "Public Bank Berhad".to_string(),
            symbol: "PBBANK".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.5
        })),
        Arc::new(Mutex::new(Stock {
            name: "CIMB Group Holdings Bhd".to_string(),
            symbol: "CIMB".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.4
        })),
        Arc::new(Mutex::new(Stock {
            name: "M & A Equity Holdings Bhd".to_string(),
            symbol: "M&A".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "Evergreen Max Cash Capital Bhd".to_string(),
            symbol: "EMCC".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.6
        })),
        Arc::new(Mutex::new(Stock {
            name: "Kenanga Investment Bank Bhd".to_string(),
            symbol: "KENANGA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "Malaysia Building Society Bhd".to_string(),
            symbol: "MBSB".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "AMMB Holdings Berhad".to_string(),
            symbol: "AMBANK".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.6
        })),
        Arc::new(Mutex::new(Stock {
            name: "RHB Bank Bhd".to_string(),
            symbol: "RHBBANK".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "Fintec Global Bhd".to_string(),
            symbol: "FINTEC".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.4
        })),
        // ------------------------ 10 Stocks ------------------------
        Arc::new(Mutex::new(Stock {
            name: "RCE Capital Bhd".to_string(),
            symbol: "RCECAP".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Bank Islam Malaysia Bhd".to_string(),
            symbol: "BIMB".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Insas Bhd".to_string(),
            symbol: "INSAS".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.3
        })),
        Arc::new(Mutex::new(Stock {
            name: "Affin Bank Bhd".to_string(),
            symbol: "AFFIN".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "Tune Protect Group Bhd".to_string(),
            symbol: "TUNEPRO".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.5
        })),
        Arc::new(Mutex::new(Stock {
            name: "Nestle (Malaysia) Berhad".to_string(),
            symbol: "NESTLE".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.5
        })),
        Arc::new(Mutex::new(Stock {
            name: "Mercury Securities Group Bhd".to_string(),
            symbol: "MERSEC".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.2
        })),
        Arc::new(Mutex::new(Stock {
            name: "Hong Leong Bank Bhd".to_string(),
            symbol: "HLBANK".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.45
        })),
        Arc::new(Mutex::new(Stock {
            name: "Alliance Bank Malaysia Bhd".to_string(),
            symbol: "ABMB".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        Arc::new(Mutex::new(Stock {
            name: "MAA Group Bhd".to_string(),
            symbol: "MAA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.33
        })),
        // ------------------------ 10 Stocks ------------------------
        Arc::new(Mutex::new(Stock {
            name: "Velesto Energy Bhd".to_string(),
            symbol: "VELESTO".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.33
        })),
        Arc::new(Mutex::new(Stock {
            name: "Dialog Group Bhd".to_string(),
            symbol: "DIALOG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.79
        })),
        Arc::new(Mutex::new(Stock {
            name: "Bumi Armada Bhd".to_string(),
            symbol: "ARMADA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.54
        })),
        Arc::new(Mutex::new(Stock {
            name: "Icon Offshore Bhd".to_string(),
            symbol: "ICON".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.73
        })),
        Arc::new(Mutex::new(Stock {
            name: "Yinson Holdings Berhad".to_string(),
            symbol: "YINSON".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.2
        })),
        Arc::new(Mutex::new(Stock {
            name: "Perdana Petroleum Bhd".to_string(),
            symbol: "PERDANA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "Sapura Energy Bhd".to_string(),
            symbol: "SAPNRG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.57
        })),
        Arc::new(Mutex::new(Stock {
            name: "Reservoir Link Energy Bhd".to_string(),
            symbol: "RL".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.63
        })),
        Arc::new(Mutex::new(Stock {
            name: "T7 Global Bhd".to_string(),
            symbol: "T7GLOBAL".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.75
        })),
        Arc::new(Mutex::new(Stock {
            name: "Dayang Enterprise Holdings Berhad".to_string(),
            symbol: "DAYANG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.7
        })),
        // ------------------------ 10 Stocks ------------------------
        Arc::new(Mutex::new(Stock {
            name: "S P Setia Bhd".to_string(),
            symbol: "SPSETIA".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "Iskandar Waterfront City Bhd".to_string(),
            symbol: "IWCITY".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "NCT Alliance Bhd".to_string(),
            symbol: "NCT".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "Eastern & Oriental Bhd".to_string(),
            symbol: "E&O".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.85
        })),
        Arc::new(Mutex::new(Stock {
            name: "Tanco Holdings Bhd".to_string(),
            symbol: "TANCO".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.82
        })),
        Arc::new(Mutex::new(Stock {
            name: "Jiankun International Bhd".to_string(),
            symbol: "JIANKUN".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.25
        })),
        Arc::new(Mutex::new(Stock {
            name: "Sime Darby Property Bhd".to_string(),
            symbol: "SIMEPROP".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.88
        })),
        Arc::new(Mutex::new(Stock {
            name: "Mah Sing Group Bhd".to_string(),
            symbol: "MAHSING".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.8
        })),
        Arc::new(Mutex::new(Stock {
            name: "Eco World Development Group Bhd".to_string(),
            symbol: "ECOWLD".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.75
        })),
        Arc::new(Mutex::new(Stock {
            name: "IOI Properties Group Bhd".to_string(),
            symbol: "IOIPG".to_string(),
            value: 100.0,
            stock_direction: "NULL".to_string(),
            volatility: 0.56
        })),
    ];

    let stocks_clone = stocks.clone();
    let stocks_clone_1 = stocks_clone.clone();

    // Step 1: Stock Selector
    stock_selector_pool.execute_at_fixed_rate(
        Duration::from_secs(0),
        Duration::from_secs(3), 
        move || {
            // Generate number to select stock
            let mut rng = rand::thread_rng();
            let r_stock = rng.gen_range(0..=stocks.len());

            // Clone Arc            
            let stock = stocks[r_stock].clone();

            // Send Stock to Incrementor
            select_sender.send(stock).unwrap();
        }
    );

    // Step 2: Stock Updater
    stock_updater_pool.execute_at_fixed_rate(
        Duration::from_secs(0),
        Duration::from_secs(4), 
        move || {
            match select_receiver.try_recv() {
                Ok(stock) => {

                    // Randomly increment or decrement stock value
                    let mut rng = rand::thread_rng();

                    let inc_dec = if rng.gen_bool(0.6) { 1 } else { -1 };
                    let value = (rng.gen_range(0..20) * inc_dec) as f32;

                    // Cap a limit (Do not let stock drop until 0)
                    if !(stock.lock().unwrap().value <= 20.0 && value < 0.0) {
                        stock.lock().unwrap().value += value;
                    }

                    // Send stock direction
                    stock.lock().unwrap().stock_direction = if inc_dec == 1 { "UP".to_string() } else { "DOWN".to_string() };

                    // Prepare to send to broker
                    broker_sender.send(stock.lock().unwrap().clone()).unwrap();
                }
                Err(_) => {
                    // Do nothing
                }
            }
        }
    );

    // Step 3: Broadcast stock to brokers through RabbitMQ
    thread::spawn(move || -> Result<()>{   
        let connection = connection_clone.clone();
        let channel = connection.lock().unwrap().open_channel(None)?;
        // Declare fanout exchange (Broadcast)
        let exchange = channel.exchange_declare(
            ExchangeType::Fanout,
            "stock_exchange",
            ExchangeDeclareOptions::default(),
        )?;

        loop {
            
            // Send initial value of stocks to brokers first
            if initial {

                for stock in stocks_clone_1.iter() {
                    let locked_stock = stock.lock().unwrap().clone();

                    let serialized = serde_json::to_string(&locked_stock).unwrap();
                    exchange.publish(Publish::new(serialized.as_bytes(), ""))?;
                }

                initial = false;

            } else {
                // Broadcast stock info to brokers
                match broker_receiver.try_recv() {
                    Ok(stock_info) => {   
     
                        // Display Stocks
                        println!("\n{}\nStock Name: {}\nStock Symbol: {}\nStock Value: {}\nStock Direction: {}\nStock Volatility: {}\n", 
                        "Bursa Malaysia Changes".green().bold(), stock_info.name, stock_info.symbol, stock_info.value, stock_info.stock_direction, stock_info.volatility);
                        
                        let serialized = serde_json::to_string(&stock_info).unwrap();
                        exchange.publish(Publish::new(serialized.as_bytes(), ""))?;
                }
                    Err(_) => {
                        // Do nothing
                    }
                }
            }
        }
    });

    // Step 4: Request / Reply Connection Between Stock Exchange and Broker
    loop {
        match broker_consumer.receiver().try_recv() {
            Ok(message) => {
                match message {
                    ConsumerMessage::Delivery(delivery) => {
                        let body = String::from_utf8_lossy(&delivery.body);
                        let (reply_to, corr_id) = match (
                            delivery.properties.reply_to(),
                            delivery.properties.correlation_id(),
                        ) {
                            (Some(r), Some(c)) => (r.clone(), c.clone()),
                            _ => {
                                println!("Received delivery without reply_to or correlation_id");
                                broker_consumer.ack(delivery)?;
                                continue;
                            }
                        };

                        // Deserealize Response
                        let broker_buysell_stock_info: BuySellStockInfo = serde_json::from_str(&body).unwrap();

                        // Buy/Sell Stock
                        let response = buy_sell_stock(stocks_clone.clone(), broker_buysell_stock_info);

                        // Send response back to broker
                        direct_exchange.publish(Publish::with_properties(
                            response.as_bytes(),
                            reply_to.clone(),
                            AmqpProperties::default().with_correlation_id(corr_id.clone()),
                        ))?;

                        broker_consumer.ack(delivery.clone())?;
                    }
                    other => {
                        println!("Broker Consumer ended: {:?}", other);
                    }
                }
            }
            Err(_) => {
                // Do nothing
            }
        }
    }
}

fn buy_sell_stock(stocks: Vec<Arc<Mutex<Stock>>>, buy_sell_info: BuySellStockInfo) -> String {

    let mut result = "".to_string();

    // Find Stock to Buy/Sell (Add On logic)
    for stock in stocks {

        let mut stock_unlocked = stock.lock().unwrap();
        
        if stock_unlocked.symbol == buy_sell_info.stock_symbol {
            println!("Stock Exchange - Broker {} {} RM {} worth of {} successfully !!!", buy_sell_info.broker_name, buy_sell_info.buy_or_sell, buy_sell_info.amount , buy_sell_info.stock_symbol);

            // Fluctuation of Stock After Buy/Sell
            let amount_stock_bought = buy_sell_info.amount / stock_unlocked.value;
            let old_stock_value = stock_unlocked.value;
            let value_change = amount_stock_bought * stock_unlocked.volatility;

            if buy_sell_info.buy_or_sell == "Buy" {
                stock_unlocked.value += value_change;
                stock_unlocked.stock_direction = "UP".to_string();
            } else {
                stock_unlocked.value -= value_change;
                stock_unlocked.stock_direction = "DOWN".to_string();

                // Reset stock back to 100 if it drops below 0
                if stock_unlocked.value < 0.0 {
                    stock_unlocked.value = 100.0;
                }
            }

            // Display Volatility Changes
            println!("\n{}\nStock Name: {}\nStock Symbol: {}\nStock Old Value:{}\nStock New Value: {}\nStock Direction: {}\nStock Volatility: {}\n", 
            "Bursa Malaysia Volatility Changes".green().bold(), stock_unlocked.name, stock_unlocked.symbol, old_stock_value, stock_unlocked.value, stock_unlocked.stock_direction, stock_unlocked.volatility);

            result = "SUCCESS".to_string();
            break;
        } else {
            result = "FAILED".to_string();
        }

    }

    // Display response
    result
}