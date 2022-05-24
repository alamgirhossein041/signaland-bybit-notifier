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
    // SimpleLogger::new()
    //     .with_utc_timestamps()
    //     .with_level(LevelFilter::Info)
    //     .init()
    //     .unwrap();

    // let func = handler_fn(func);
    // lambda_runtime::run(func).await?;

    let x = r#"
{
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "execution_stream",
                "sequenceNumber": "49629772145300376766238595238381810977688592954102382594",
                "data": "WyIyYzBkODdlOC0xZDc1LTRhNzgtYjJjZC1iYWViNTc3NTcyZmMiLCIxNjg0NzU1NDE3NDI2Iiwie1widG9waWNcIjpcImV4ZWN1dGlvblwiLFwiZGF0YVwiOlt7XCJzeW1ib2xcIjpcIlBFT1BMRVVTRFRcIixcInNpZGVcIjpcIkJ1eVwiLFwib3JkZXJfaWRcIjpcIjhiYjE0MWMwLTBlNmMtNDIyNC04NTI1LTgyODE1MDUyMzJlZVwiLFwiZXhlY19pZFwiOlwiZDlhODEzZDMtZWQyNy01YzM3LWE5ZDUtYTEyNjgyOTE1MGQ3XCIsXCJvcmRlcl9saW5rX2lkXCI6XCJcIixcInByaWNlXCI6MC4wMjYxNSxcIm9yZGVyX3F0eVwiOjMwOTI4LFwiZXhlY190eXBlXCI6XCJUcmFkZVwiLFwiZXhlY19xdHlcIjoyMjQ5Ni45OTgsXCJleGVjX2ZlZVwiOjAuMzUyOTc3OTMsXCJsZWF2ZXNfcXR5XCI6ODQzMSxcImlzX21ha2VyXCI6ZmFsc2UsXCJ0cmFkZV90aW1lXCI6XCIyMDIyLTA1LTI0VDA5OjEyOjA2LjE4OTg4MlpcIn0se1wic3ltYm9sXCI6XCJQRU9QTEVVU0RUXCIsXCJzaWRlXCI6XCJCdXlcIixcIm9yZGVyX2lkXCI6XCI4YmIxNDFjMC0wZTZjLTQyMjQtODUyNS04MjgxNTA1MjMyZWVcIixcImV4ZWNfaWRcIjpcImY3ZjI1OTg2LTgzMzQtNWVmNS1iODcwLWVkMzcyYzBkNjNjNVwiLFwib3JkZXJfbGlua19pZFwiOlwiXCIsXCJwcmljZVwiOjAuMDI2MTUsXCJvcmRlcl9xdHlcIjozMDkyOCxcImV4ZWNfdHlwZVwiOlwiVHJhZGVcIixcImV4ZWNfcXR5XCI6ODQzMSxcImV4ZWNfZmVlXCI6MC4xMzIyODIzOSxcImxlYXZlc19xdHlcIjowLFwiaXNfbWFrZXJcIjpmYWxzZSxcInRyYWRlX3RpbWVcIjpcIjIwMjItMDUtMjRUMDk6MTI6MDYuMTg5ODgyWlwifV19Il0=",
                "approximateArrivalTimestamp": 1653383526.313
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49629772145300376766238595238381810977688592954102382594",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::995606098483:role/service-role/tester-kinesis-role-hg6xq0f0",
            "awsRegion": "eu-central-1",
            "eventSourceARN": "arn:aws:kinesis:eu-central-1:995606098483:stream/signaland-vip-websocket-notifications"
        },
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "execution_stream",
                "sequenceNumber": "49629772145300376766238595665346605171726150185395421186",
                "data": "WyJjYThiNjEzYi05Njc3LTRmZGUtOWE5MS00YzFlZWZiNTAyNDIiLCIzMzMuODQ2OTc3NjE1NDIwOTQiLCJ7XCJ0b3BpY1wiOlwiZXhlY3V0aW9uXCIsXCJkYXRhXCI6W3tcInN5bWJvbFwiOlwiV09PVVNEVFwiLFwic2lkZVwiOlwiQnV5XCIsXCJvcmRlcl9pZFwiOlwiMzNhOTZlYmMtMjVjMy00ZTBjLTg3YjEtYzRkZjViNGQwYmQ0XCIsXCJleGVjX2lkXCI6XCJkODY2Njg4Yy0zMDU5LTUwOGQtYjA2Ny0wNDNmZGQ4MWZmNWRcIixcIm9yZGVyX2xpbmtfaWRcIjpcIlRQXzU0NDY2MjE4MDExODI5NjAwOThcIixcInByaWNlXCI6MC4xOTg1LFwib3JkZXJfcXR5XCI6NzQyLjIsXCJleGVjX3R5cGVcIjpcIlRyYWRlXCIsXCJleGVjX3F0eVwiOjc0Mi4yLFwiZXhlY19mZWVcIjowLjAxNDczMjY3LFwibGVhdmVzX3F0eVwiOjAsXCJpc19tYWtlclwiOnRydWUsXCJ0cmFkZV90aW1lXCI6XCIyMDIyLTA1LTI0VDE1OjU2OjU1LjYxODY5NVpcIn1dfSJd",
                "approximateArrivalTimestamp": 1653407815.96
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49629772145300376766238595665346605171726150185395421186",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::995606098483:role/service-role/tester-kinesis-role-hg6xq0f0",
            "awsRegion": "eu-central-1",
            "eventSourceARN": "arn:aws:kinesis:eu-central-1:995606098483:stream/signaland-vip-websocket-notifications"
        }
    ]
}
    "#;

    let val: Value = json_decode(x).unwrap();

    func(val, Context::default()).await;

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
