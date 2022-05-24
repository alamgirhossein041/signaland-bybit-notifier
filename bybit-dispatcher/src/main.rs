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
    strategies::Strategies,
};
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // SimpleLogger::new()
    //     .with_utc_timestamps()
    //     .with_level(LevelFilter::Info)
    //     .init()
    //     .unwrap();

    // let func = handler_fn(func);
    // lambda_runtime::run(func).await?;

    let data = r#"
    {
        "Records": [
            {
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "execution_stream",
                    "sequenceNumber": "49629772145300376766238594105929011934417023482330087426",
                    "data": "WyIyYzBkODdlOC0xZDc1LTRhNzgtYjJjZC1iYWViNTc3NTcyZmMiLCIxNjg0NzU1NDE3NDI2Iiwie1widG9waWNcIjpcImV4ZWN1dGlvblwiLFwiZGF0YVwiOlt7XCJzeW1ib2xcIjpcIk9DRUFOVVNEVFwiLFwic2lkZVwiOlwiQnV5XCIsXCJvcmRlcl9pZFwiOlwiMWYwMzZjYjQtMGZkZS00MWNkLTliNzMtYzYyMGZjZDI1NjY0XCIsXCJleGVjX2lkXCI6XCI1NzFmNTM1Mi01NDNlLTU1MmEtYWJhYy1kYTlmODhlNjMwZWZcIixcIm9yZGVyX2xpbmtfaWRcIjpcIlwiLFwicHJpY2VcIjowLjIzNDcsXCJvcmRlcl9xdHlcIjozMTg1Ljk5OTgsXCJleGVjX3R5cGVcIjpcIlRyYWRlXCIsXCJleGVjX3F0eVwiOjI1MDAsXCJleGVjX2ZlZVwiOjAuMzUyMDUsXCJsZWF2ZXNfcXR5XCI6Njg2LFwiaXNfbWFrZXJcIjpmYWxzZSxcInRyYWRlX3RpbWVcIjpcIjIwMjItMDUtMjNUMjI6MDg6MzkuMzM2OTk0WlwifSx7XCJzeW1ib2xcIjpcIk9DRUFOVVNEVFwiLFwic2lkZVwiOlwiQnV5XCIsXCJvcmRlcl9pZFwiOlwiMWYwMzZjYjQtMGZkZS00MWNkLTliNzMtYzYyMGZjZDI1NjY0XCIsXCJleGVjX2lkXCI6XCJhYjYwZTM3Ni0yYjc1LTVjMWUtYTQ2My05YmM1YWU0MjYzZjNcIixcIm9yZGVyX2xpbmtfaWRcIjpcIlwiLFwicHJpY2VcIjowLjIzNDgsXCJvcmRlcl9xdHlcIjozMTg1Ljk5OTgsXCJleGVjX3R5cGVcIjpcIlRyYWRlXCIsXCJleGVjX3F0eVwiOjY4NixcImV4ZWNfZmVlXCI6MC4wOTY2NDM2OCxcImxlYXZlc19xdHlcIjowLFwiaXNfbWFrZXJcIjpmYWxzZSxcInRyYWRlX3RpbWVcIjpcIjIwMjItMDUtMjNUMjI6MDg6MzkuMzM2OTk0WlwifV19Il0=",
                    "approximateArrivalTimestamp": 1653310875.388
                },
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": "shardId-000000000000:49629772145300376766238594105929011934417023482330087426",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": "arn:aws:iam::995606098483:role/service-role/tester-kinesis-role-hg6xq0f0",
                "awsRegion": "eu-central-1",
                "eventSourceARN": "arn:aws:kinesis:eu-central-1:995606098483:stream/signaland-vip-websocket-notifications"
            }
        ]
    }
    "#;

    let val: Value = json_decode(data).unwrap();
    // let order_evt: OrderEvent = OrderEvent::new(val);

    func(val, Context::default()).await;

    println!("OK");
    Ok(())
}

async fn func(event: Value, _: Context) -> Result<Value, Error> {
    let evt = OrderEvent::new(event);

    if evt.is_close() {
        // Retrieve Opening
        let open = OrderStore::fetch_last_order(
            "signaland_users_orders",
            *&evt.stream_id.parse::<u64>().unwrap(),
            &evt.symbol(),
        )
        .await;

        if open.is_none() {
            warn!(
                "{}",
                format!(
                    "Error while retrieving the opening of this order {}",
                    json_encode(&evt.data).unwrap()
                )
            );
        } else {
            let open = open.unwrap();
            // Calculate PnL
            let open_order: Vec<OrderWS> = json_decode(&open.data).unwrap();
            let initial_qty: f64 = open_order.iter().map(|x| x.price).sum::<f64>()
                / open_order.len() as f64
                * open_order.iter().map(|x| x.exec_qty).sum::<f64>();
            let final_qty: f64 = evt.price() * evt.quantity();
            let fees: f64 = evt.fees() + open_order.iter().map(|x| x.exec_fee).sum::<f64>();

            let pnl: f64 = if &open_order.first().unwrap().side == "Buy" {
                final_qty - initial_qty - fees
            } else {
                initial_qty - final_qty - fees
            };

            // Store PnL
            let strategy = if &open_order.first().unwrap().side == "Buy" {
                Strategies::LongTonic
            } else {
                Strategies::ShortColada
            };

            let user_pnl = UserPnL::new(
                evt.user_id.clone(),
                Exchange::BYBIT,
                evt.symbol().clone(),
                strategy,
                pnl,
            );
            user_pnl.store("signaland_users_pnl").await;
        }
    }

    let order_store = OrderStore::new(
        evt.user_id.clone(),
        evt.stream_id.parse::<u64>().unwrap(),
        Exchange::BYBIT,
        evt.symbol().clone(),
        json_encode(&evt.data).unwrap(),
    );

    order_store.store_on_dynamo("signaland_users_orders").await;

    Ok(json!(format!("DONE!")))
}

#[derive(Serialize, Deserialize)]
pub struct OrderEvent {
    pub user_id: String,
    pub stream_id: String,
    pub data: Vec<OrderWS>,
}

impl OrderEvent {
    pub fn new(input: Value) -> OrderEvent {
        let event = input["Records"]
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_object()
            .unwrap()
            .get("kinesis")
            .unwrap();

        let kinesis_data = event.get("data").unwrap().as_str().unwrap();

        let data = String::from_utf8(base64::decode(kinesis_data).unwrap()).unwrap();

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
