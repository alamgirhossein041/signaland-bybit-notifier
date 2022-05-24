use lambda_runtime::{handler_fn, Context, Error};
use log::{warn, LevelFilter};
use serde::{Deserialize, Serialize};
use serde_json::{from_str as json_decode, json, to_string as json_encode, Value};
use signaland_rust_lib::signaland::{
    models::trading::brokers::Exchange,
    services::{
        exchange::bybit::types::{OrderWS, WSResponse},
        users::store::{OrderStore, UserPnL},
    },
};
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Error> {
    SimpleLogger::new()
        .with_utc_timestamps()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let func = handler_fn(func);
    lambda_runtime::run(func).await?;

    println!("OK");
    Ok(())
}

async fn func(event: Value, _: Context) -> Result<Value, Error> {
    // Event could have more than one kinesis order, separate orders and

    let records = event["Records"].as_array().unwrap();

    for r in records {
        let record = r.as_object().unwrap().get("kinesis").unwrap();

        let data = String::from_utf8(
            base64::decode(record.get("data").unwrap().as_str().unwrap()).unwrap(),
        )
        .unwrap();

        let order_event = OrderEvent::new(data);

        // If it's an opening store to PNL
        if order_event.is_open() {
            let pnl = UserPnL::new(
                order_event.user_id.clone(),
                Exchange::BYBIT,
                order_event.symbol(),
                order_event.stream_id.parse::<u64>().unwrap(),
            );

            pnl.store("signaland_users_pnl").await;
        }

        for evt in order_event.data {
            let order_store = OrderStore::new(
                evt.order_id.clone(),
                evt.order_link_id.clone(),
                order_event.user_id.clone(),
                order_event.stream_id.parse::<u64>().unwrap_or(0),
                Exchange::BYBIT,
                evt.exec_qty,
                evt.price,
                evt.leaves_qty,
                evt.symbol.clone(),
                json_encode(&evt).unwrap(),
            );

            order_store.store_on_dynamo("signaland_users_orders").await;
        }
    }

    Ok(json!(format!("DONE!")))
}

#[derive(Serialize, Deserialize)]
pub struct OrderEvent {
    pub user_id: String,
    pub stream_id: String,
    pub data: Vec<OrderWS>,
}

impl OrderEvent {
    pub fn new(data: String) -> OrderEvent {
        let decoded: (String, String, String) = json_decode(&data).unwrap();
        let orders_data: WSResponse<OrderWS> = json_decode(&decoded.2).unwrap();

        OrderEvent {
            user_id: decoded.0,
            stream_id: decoded.1,
            data: orders_data.data,
        }
    }

    pub fn symbol(&self) -> String {
        self.data.first().unwrap().symbol.clone()
    }

    pub fn fees(&self) -> f64 {
        self.data.iter().map(|x| x.exec_fee).sum::<f64>()
    }

    pub fn price(&self) -> f64 {
        let leaves = self.data.len() as f64;
        let prices: f64 = self.data.iter().map(|x| x.price).sum();
        prices / leaves
    }

    pub fn quantity(&self) -> f64 {
        self.data.iter().map(|x| x.exec_qty).sum()
    }

    pub fn is_open(&self) -> bool {
        !self.data.first().unwrap().order_link_id.is_empty()
            && !self.data.first().unwrap().order_link_id.contains("TP_")
    }

    pub fn is_close(&self) -> bool {
        !self.is_open()
    }
}
