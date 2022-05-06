mod lib;

use std::{
    thread, time, collections::HashMap, 
    sync::mpsc::{SyncSender, sync_channel},
    sync::{Arc, Mutex}
};

use lib::user::user::get_users_bybit_streams;

use signaland_rust_lib::signaland as signaland;
use signaland_rust_lib::signaland::services::aws::kinesis::push as push_to_kinesis;
use signaland::services::exchange::bybit::{
    api::websocket::AuthenticatedDerivativesAccount
};

use serde_json::to_string;

type Connections = Arc<Mutex<HashMap<(String, String), AuthenticatedDerivativesAccount>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // User hashmap Schema => HASH<(UserID, StreamId), (ApiKey, ApiSecret)>
    let connections: Connections = Arc::new(Mutex::new(HashMap::new()));

    // Populate users
    let users = Arc::clone(&connections);
    tokio::spawn(async move {
        loop {
            let accounts = get_users_bybit_streams().await;
            let mut user_connections = users.lock().unwrap();
            for ((user_id, stream_id), (key, secret)) in accounts {
                log::debug!("Connected User {}, with Stream {}", user_id, stream_id);
                *user_connections
                    .entry((user_id, stream_id))
                    .or_insert(AuthenticatedDerivativesAccount::new_with_subscriptions(&key, &secret, vec![String::from("execution")])) 
                    = AuthenticatedDerivativesAccount::new_with_subscriptions(&key, &secret, vec![String::from("execution")]);
            }
            drop(user_connections);

            thread::sleep(time::Duration::from_secs(5*60));
        }

    });

    // Message sender to Kinesis
    let (execution_sender, execution_receiver): (SyncSender<(String, String, String)>, _) = sync_channel(2048);

    tokio::spawn(async move {
        loop {
            let message = execution_receiver.try_recv();
            if message.is_err() { continue } // TODO: handle this error

            let raw_data = message.unwrap();

            push_to_kinesis(
                "signaland-vip-websocket-notifications", 
                bytes::Bytes::from(serde_json::to_string(&raw_data).unwrap()),
                "execution_stream"
            ).await;
        }
    });

    // thread to ping pong
    let (ping_sender, ping_receiver) : (SyncSender<i8>, _) = sync_channel(1);
    // Execution message is: UserId, StreamId, ByBit Execution Message
    
    std::thread::spawn(move || {
        loop {
            ping_sender.send(0).unwrap();
            thread::sleep(time::Duration::from_secs(10));
        }
    });

    // Main Loop
    let socket_connections = Arc::clone(&connections);

    loop {
        // HeartBeat Ping Pong Websocket Handler
        let rev_message = ping_receiver.try_recv();
        let mut user_connections = socket_connections.lock().unwrap();
        if rev_message.is_ok() {
            for ((_user_id, _stream_id), auth) in &mut *user_connections {
                auth.ping();
            }
        }

        for ((user_id, stream_id), auth) in &mut *user_connections {
            let incoming = auth.private_stream.read_message();
            match incoming {
                Result::Ok(message) => { 
                    // Avoid to send messages that does not have correct body
                    let data = message.into_text().unwrap();
                    println!("{:?}", data);
                    if data.contains("order_id") {         
                        execution_sender
                            .send((user_id.clone(), stream_id.clone(), String::from(data)))
                            .unwrap(); 
                    }
                    () 
                },
                Result::Err(_e) => { continue }
            }
        }

        drop(user_connections)
    }
    

    Ok(())
}
