extern crate prettytable;

use uuid::Uuid;
use std::thread;
use colored::Colorize;
use serde::{Deserialize, Serialize};
use prettytable::{Table, Row, Cell};
use std::sync::{mpsc::channel, Arc, Mutex};
use amiquip::{AmqpProperties, Connection, ConsumerMessage, ConsumerOptions, Exchange, ExchangeDeclareOptions, ExchangeType, FieldTable, Publish, QueueDeclareOptions, Result};

use crate::{client::ClientStockPreference, stock_exchange::Stock};

#[derive(Debug, Clone)]
pub struct StockAnalysis {
    pub stock_symbol: String,
    pub price: f32,
    pub recent_trend: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuySellStockInfo {
    pub stock_symbol: String,
    pub broker_name: String,
    pub buy_or_sell: String,
    pub amount: f32,
}

pub fn broker(broker_number: String, red: u8, green: u8, blue: u8) -> Result<(), amiquip::Error> {

    let starting_response = format!("Broker {} has started !!!\n", broker_number.clone()).to_string();

    println!("{}", starting_response.truecolor(red, green, blue));

    // -------------------- RabbitMQ --------------------

    // Establish connection to RabbitMQ
    let connection = Arc::new(Mutex::new(Connection::insecure_open("amqp://guest:guest@localhost:5672")?));
    let connection_clone = connection.clone();
    let connection_clone_1 = connection.clone();

    // --------------------------------------------------

    let trend_history: Arc<Mutex<Vec<StockAnalysis>>> = Arc::new(Mutex::new(Vec::new()));
    let trend_history_clone = trend_history.clone();
    let trend_history_clone_1 = trend_history.clone();

    let broker_number_clone = broker_number.clone();
    let broker_number_clone_1 = broker_number.clone();

    // Vector to store client preferences
    let client_preferences = Arc::new(Mutex::new(Vec::new()));
    let client_preferences_clone = client_preferences.clone();

    let (check_if_stock_available_sender, check_if_stock_available_receiver) = channel();
    let check_if_stock_available_sender_clone = check_if_stock_available_sender.clone();
    
    let order_table_header = vec!["Client", "Desired Stock", "Buy/Sell", "Price", "Criteria", "Info"];

    // Step 1: Receive Broadcast Messages From Stock Exchange and Analyze Stock Trend
    thread::spawn(move || -> Result<()>{
        let connection = connection_clone;

        let channel = connection.lock().unwrap().open_channel(None)?;

        let exchange = channel.exchange_declare(
            ExchangeType::Fanout,
            "stock_exchange",
            ExchangeDeclareOptions::default(),
        )?;

        let queue = channel.queue_declare(broker_number.to_string(), QueueDeclareOptions::default())?;

        let _ = queue.purge();

        queue.bind(&exchange, "", FieldTable::new())?;

        // Start a consumer. Use no_ack: true so the server doesn't wait for us to ack the messages it sends us
        let consumer = queue.consume(ConsumerOptions {
            no_ack: true,
            ..ConsumerOptions::default()
        })?;

        // Receive Broadcast Message
        for (_, message) in consumer.receiver().iter().enumerate() {
            
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&delivery.body);
                        
                    let received_stock = body.to_string(); // Convert Received Stock to String

                    // Deserialize Stock
                    let deseralized_stock: Result<Stock, serde_json::Error> = serde_json::from_str(&received_stock);

                    match deseralized_stock {
                        Ok(stock) => {
                            // Analyze Stock Trends
                            analyze_stock( stock, trend_history_clone.clone());

                            // After broker analyze stock, Send message to receiver to start and anaylze if can buy stock for user
                            check_if_stock_available_sender_clone.send("Start").unwrap();
                        }
                        Err(_) => {
                            // Do nothing
                        }
                    }
                }
                _ => {
                    println!("{} ended", broker_number);
                    break;
                }
            }
        }

        Ok(())
    });

    // Step 2: Receive Message from Client
    thread::spawn(move || -> Result<()>{

        let channel = connection_clone_1.lock().unwrap().open_channel(None)?;

        // Declare the queue that will receive Client requests.
        let queue = channel.queue_declare(
            format!("broker_{}_client_queue", broker_number_clone).to_string(), 
            QueueDeclareOptions::default()
        )?;

        // Start Consumer
        let broker_client_consumer = queue.consume(ConsumerOptions::default())?;

        for (_, message) in broker_client_consumer.receiver().iter().enumerate() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);

                    // Deserialize Response
                    let client_stock_preference: ClientStockPreference = serde_json::from_str(&body).unwrap();

                    // Add it to vector to be used later
                    client_preferences_clone.lock().unwrap().push(client_stock_preference);

                    // --------------------------------- TABLE DISPLAY ---------------------------------

                    // Broker's table of orders
                    let mut table = Table::new();

                    // Add headers
                    table.add_row(Row::new(
                        order_table_header.iter().map(|header| Cell::new(header)).collect()
                    ));

                    // Add Content to Table
                    for client_preference in client_preferences_clone.lock().unwrap().iter() {
                        let response = structure_client_request_message(client_preference.clone());

                        table.add_row(Row::new(vec![
                            Cell::new(&client_preference.client_number),
                            Cell::new(&client_preference.stock_symbol),
                            Cell::new(&client_preference.buy_or_sell),
                            Cell::new(&client_preference.amount.to_string()),
                            Cell::new(&client_preference.buy_sell_decision),
                            Cell::new(&response),
                        ]));

                    }

                    println!("\n{}", format!("Broker {}'s Orders:", broker_number_clone).truecolor(red, green, blue));
                    table.printstd();
                    println!("\n");

                    // ----------------------------------------------------------------------------------

                    // Send message to receiver to start and anaylze if can buy stock for user
                    check_if_stock_available_sender.send("Start").unwrap();

                    broker_client_consumer.ack(delivery)?;

                }
                other => {
                    println!("Broker-Client Consumer ended: {:?}", other);
                    break;
                }
            }
        }

        Ok(())
    });

    loop {
        match check_if_stock_available_receiver.try_recv() {
            Ok(_) => {

                // Check whether can buy stock for users
                check_client_preference(broker_number_clone_1.clone(), trend_history_clone_1.clone(), client_preferences.clone());

            }
            Err(_) => {
                // Do Nothing
            }
        }
    }
}

fn check_client_preference( broker_number: String, trend_history: Arc<Mutex<Vec<StockAnalysis>>>,  client_preferences: Arc<Mutex<Vec<ClientStockPreference>>>) {
    let broker_number = broker_number.clone();

    let stock_information = trend_history.lock().unwrap();

    let mut indexes_to_remove = Vec::new(); // To store indexes to remove

    // If vector is not empty
    for (index, client_preference) in client_preferences.lock().unwrap().iter().enumerate() {

        // Compare client's preferences with stock trends
        for stock in stock_information.iter() {
            if stock.stock_symbol == client_preference.stock_symbol {
                
                let mut response: Result<String, amiquip::Error> = Ok("".to_string());

                // Client wants to buy based on trend
                if client_preference.buy_sell_decision == "Trend" && stock.recent_trend.len() == 2 {
                    if stock.recent_trend[0] == client_preference.trend[0] && stock.recent_trend[1] == client_preference.trend[1] {

                        // Create BuySellStockInfo object
                        let buy_sell_stock_info = BuySellStockInfo {
                            stock_symbol: stock.stock_symbol.clone(),
                            broker_name: broker_number.clone(),
                            buy_or_sell: client_preference.buy_or_sell.clone(),
                            amount: client_preference.amount.clone(),
                        };
        
                        // Buy/Stock Function
                        response = send_request_stock_exchange(buy_sell_stock_info);

                    }
                } 
                // Client wants to buy based on stock price
                else if client_preference.buy_sell_decision == "Price" {
                    // Buy stock if it is lower than client's price
                    // Sell stock if it is higher than client's price
                    if (stock.price < client_preference.min_price && client_preference.buy_or_sell == "Buy") || (stock.price > client_preference.min_price && client_preference.buy_or_sell == "Sell") {
                        
                        // Create BuySellStockInfo object
                        let buy_sell_stock_info = BuySellStockInfo {
                            stock_symbol: stock.stock_symbol.clone(),
                            broker_name: broker_number.clone(),
                            buy_or_sell: client_preference.buy_or_sell.clone(),
                            amount: client_preference.amount.clone(),
                        };
        
                        // Buy/Stock Function
                        response = send_request_stock_exchange(buy_sell_stock_info);
                    }
                } 
                // Client just wants to buy based on the stock symbol
                else if client_preference.buy_sell_decision == "Symbol" {
                    // Create BuySellStockInfo object
                    let buy_sell_stock_info = BuySellStockInfo {
                        stock_symbol: stock.stock_symbol.clone(),
                        broker_name: broker_number.clone(),
                        buy_or_sell: client_preference.buy_or_sell.clone(),
                        amount: client_preference.amount.clone(),
                    };
    
                    // Buy/Stock Function
                    response = send_request_stock_exchange(buy_sell_stock_info);
                }

                let response_clone = response.unwrap().clone();

                if response_clone != "" {
                    // Reply back to client
                    match reply_to_client(client_preference.clone(), broker_number.clone(), response_clone.clone()) {
                        Ok(_) => {
                            indexes_to_remove.push(index);
                            break;
                        }
                        Err(_) => {
                            println!("{}", "ERROR: Failed to reply to client".red().bold());
                        }
                    
                    };
                }
            }
        }
    }

    for &index in indexes_to_remove.iter().rev() {
        client_preferences.lock().unwrap().remove(index);
    }

}

fn reply_to_client(client_preference: ClientStockPreference, broker_number: String, response_from_stock_exchange: String) -> Result<()>{

    // Create Direct Exchange to Client to send response
    
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;
    
    let channel = connection.open_channel(None)?;

    let exchange = Exchange::direct(&channel);

    let response: String;

    // Create response
    if response_from_stock_exchange == "SUCCESS" {
        response = format!("Broker {} successfully {} {} for Client {} at RM {}!", broker_number, client_preference.buy_or_sell, client_preference.stock_symbol, client_preference.client_number, client_preference.amount);
    } else {
        response = format!("Broker {}: failed to buy {} for Client {}!", broker_number, client_preference.stock_symbol, client_preference.client_number);
    }

    exchange.publish(Publish::new(response.as_bytes(), format!("client_{}_response", client_preference.client_number)))?;

    connection.close()

}

fn analyze_stock(stock_info: Stock, trend_history_arc: Arc<Mutex<Vec<StockAnalysis>>>) {

    // Check if stock is in trend history
    let mut stock_in_history = false;

    let mut trend_history = trend_history_arc.lock().unwrap();
    
    for stock in trend_history.iter() {
        if stock.stock_symbol == stock_info.symbol {
            stock_in_history = true;
            break;
        }
    }
    
    if !stock_in_history {
        // Create new stock analysis object
        let new_stock = StockAnalysis {
            stock_symbol: stock_info.symbol,
            price: stock_info.value,
            recent_trend: vec![stock_info.stock_direction],
        };

        trend_history.push(new_stock);
        
    } else {
        // Update to new price & Add latest trend direction
        for stock in trend_history.iter_mut() {
            if stock.stock_symbol == stock_info.symbol {
                stock.price = stock_info.value; // Update to latest price
                stock.recent_trend.push(stock_info.stock_direction.clone()); // Add latest trend direction
                
                // Remove oldest trend direction if more than 2
                if stock.recent_trend.len() > 2 {
                    stock.recent_trend.remove(0);
                }
            }
        }
    }
    
}

fn send_request_stock_exchange(buy_sell_stock_info: BuySellStockInfo) -> Result<String> {
    // Rabbit MQ
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    let channel = connection.open_channel(None)?;

    // Create direct exchange
    let exchange = Exchange::direct(&channel);

    // Create queue to receive reply from stock exchange
    let queue = channel.queue_declare(
        "",
        QueueDeclareOptions {
            exclusive: true,
            ..QueueDeclareOptions::default()
        },
    )?;

    // Consumer to consume messages from the queue
    let consumer = queue.consume(ConsumerOptions {
        no_ack: true,
        ..ConsumerOptions::default()
    })?;

    let correlation_id = format!("{}", Uuid::new_v4());

    let serialized = serde_json::to_string(&buy_sell_stock_info).unwrap();

    exchange.publish(Publish::with_properties(
        serialized.as_bytes(),
        "buy_sell_stock_queue",
        AmqpProperties::default()
        .with_reply_to(queue.name().to_string())
        .with_correlation_id(correlation_id.clone()),
    ))?;

    for message in consumer.receiver().iter() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                if delivery.properties.correlation_id().as_ref() == Some(&correlation_id) {
                    let body = String::from_utf8_lossy(&delivery.body);
                    
                   return Ok(body.to_string());
                }
            }
            other => {
                println!("Broker ended: {:?}", other);
            }
        }
    }

    Ok("No response from stock exchange".to_string())
}

fn structure_client_request_message(client_stock_preference: ClientStockPreference) -> String{
    
    let mut output = "".to_string();

    // Different Output for different buy/sell decisions
    if client_stock_preference.buy_sell_decision == "Symbol" {

        output = "No Criteria".to_string();

    } else if client_stock_preference.buy_sell_decision == "Trend" {

        output = format!("{}, {}", 
            client_stock_preference.trend[0],
            client_stock_preference.trend[1],
        );

    } else if client_stock_preference.buy_sell_decision == "Price" {

        output = match client_stock_preference.buy_or_sell.as_str() {
            "Buy" => format!("Below {}", client_stock_preference.min_price),
            "Sell" => format!("Above {}", client_stock_preference.min_price),
            _ => String::new(), // Handle other cases if necessary
        };

    }

    output
}
