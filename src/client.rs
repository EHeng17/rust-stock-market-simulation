use std::{time::Duration, thread};
use serde::{Serialize, Deserialize};
use std::sync::{mpsc::channel, Arc, Mutex};
use rand::{thread_rng, Rng, seq::SliceRandom};
use scheduled_thread_pool::ScheduledThreadPool;
use amiquip::{Connection, ConsumerOptions, ConsumerMessage, Exchange, Publish, QueueDeclareOptions, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientStockPreference {
    pub client_number: String,
    pub stock_symbol: String,
    pub min_price: f32,
    pub trend: Vec<String>,
    pub buy_sell_decision: String,
    pub buy_or_sell: String,
    pub amount: f32,
}

pub fn client(client_number: String, broker_number: String) -> Result<()> {
    println!("Client {} started\n", client_number);

    // -------------------- RabbitMQ --------------------

    // Establish connection to RabbitMQ
    let connection = Arc::new(Mutex::new(Connection::insecure_open("amqp://guest:guest@localhost:5672")?));
    let connection_clone = connection.clone();
    let connection_clone_1 = connection.clone();

    // --------------------------------------------------

    let client_number_clone = client_number.clone();
    let order_generator_pool = ScheduledThreadPool::new(1);
    let (order_sender, order_receiver) = channel();

    let mut rng = thread_rng();

    // Step 1: Generate Random Order
    order_generator_pool.execute_at_fixed_rate(
        Duration::from_secs(rng.gen_range(2..=6)), 
        Duration::from_secs(rng.gen_range(15..=20)),  // Create order every specified seconds
        move ||  { 
            let order = generate_client_stock_preference(client_number.clone());

            order_sender.send(order).unwrap();
        }
    );

    // Step 2: Send Order to Broker (Producer)
    thread::spawn(move || -> Result<()> {
        loop {
            match order_receiver.recv() {
                Ok(order) => {
                    let connection = connection_clone.clone();

                    // Open Chanel
                    let channel = connection.lock().unwrap().open_channel(None)?;
            
                    // Create direct exchange
                    let exchange = Exchange::direct(&channel);
                        
                    // Serialize the data
                    let serialized = serde_json::to_string(&order).unwrap();
            
                    // Send it to the respective broker's routing key
                    exchange.publish(Publish::new(serialized.as_bytes(), format!("broker_{}_client_queue", broker_number).to_string()))?;
                }
                Err(_) => {
                    // Do nothing
                }
            }
        }        
    });

    // Step 3: Receive Message From Broker (Consumer)
    thread::spawn(move || -> Result<()> {
        let connection = connection_clone_1.clone();

        let channel = connection.lock().unwrap().open_channel(None).unwrap();
    
        let queue = channel.queue_declare(format!("client_{}_response", client_number_clone), QueueDeclareOptions::default())?;

        // Start a consumer.
        let consumer = queue.consume(ConsumerOptions::default())?;

        for (_, message) in consumer.receiver().iter().enumerate() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let body = String::from_utf8_lossy(&delivery.body);
                    println!("Client {} - {} ", client_number_clone, body);
                    consumer.ack(delivery)?;
                }
                other => {
                    println!("Client {} Consumer ended: {:?}", client_number_clone, other);
                    break;
                }
            }
        }

        Ok(())
    });

    // Prevent thread from exiting
    loop {}
}

pub fn generate_client_stock_preference(client_number: String) -> ClientStockPreference {
    let mut rng = thread_rng();

    let stock_symbols = [
        "HONGSENG", 
        "LAMBO", 
        "MMAG", 
        "MYEG",
        "NETX",
        "ASDION",
        "CTOS",
        "CLOUDPT",
        "EDUSPEC",
        "HTPADU",
        "SSB8",
        "BPURI",
        "SENDAI",
        "TCS",
        "JTGROUP",
        "WIDAD",
        "GAMUDA",
        "PETGAS",
        "MUHIBAH",
        "ECONBHD",
        "TOPGLOV",
        "PBBANK",
        "CIMB",
        "M&A",
        "EMCC",
        "KENANGA",
        "MBSB",
        "AMBANK",
        "RHBBANK",
        "FINTEC",
        "RCECAP",
        "BIMB",
        "INSAS",
        "AFFIN",
        "TUNEPRO",
        "NESTLE",
        "MERSEC",
        "HLBANK",
        "ABMB",
        "MAA",
        "VELESTO",
        "DIALOG",
        "ARMADA",
        "ICON",
        "YINSON",
        "PERDANA",
        "SAPNRG",
        "RL",
        "T7GLOBAL",
        "DAYANG",
        "SPSETIA",
        "IWCITY",
        "NCT",
        "E&O",
        "TANCO",
        "JIANKUN",
        "SIMEPROP",
        "MAHSING",
        "ECOWLD",
        "IOIPG",
    ];

    let stock_symbol = stock_symbols.choose(&mut rng).unwrap().to_string();

    let min_price = rng.gen_range(50..=150) as f32;

    let trends = vec![
        vec![String::from("UP"), String::from("DOWN")],
        vec![String::from("DOWN"), String::from("UP")],
        vec![String::from("UP"), String::from("UP")],
        vec![String::from("DOWN"), String::from("DOWN")],
    ];
    let trend = trends.choose(&mut rng).unwrap().to_vec();

    let buy_sell_decisions = vec!["Symbol", "Price", "Trend"];
    let buy_sell_decision = buy_sell_decisions.choose(&mut rng).unwrap().to_string();

    let buy_or_sell = if rng.gen_bool(0.5) {
        String::from("Buy")
    } else {
        String::from("Sell")
    };

    // Client will buy or sell between 10k to 20k
    let amount = rng.gen_range(5000..=10000) as f32;

    ClientStockPreference {
        client_number,
        stock_symbol,
        min_price,
        trend,
        buy_sell_decision,
        buy_or_sell,
        amount
    }
}
