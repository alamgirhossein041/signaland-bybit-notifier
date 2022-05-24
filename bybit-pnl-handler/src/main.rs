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
    let pending = UserPnL::pending_pnl("signaland_users_pnl").await;

    for mut pnl in pending {
        let orders = OrderStore::fetch_last_orders(
            "signaland_users_orders",
            pnl.stream_id,
            &pnl.pair,
            pnl.timestamp,
        )
        .await;

        let open_order = orders
            .iter()
            .filter(|x| !x.signal_id.is_empty() && !x.signal_id.contains("TP_"))
            .collect::<Vec<&OrderStore>>();

        let mut close_orders = orders
            .iter()
            .filter(|x| x.signal_id.is_empty() || x.signal_id.contains("TP_"))
            .collect::<Vec<&OrderStore>>();

        if close_orders.is_empty() {
            continue;
        }

        let open_qty = *&open_order.iter().map(|x| x.exec_qty).sum::<f64>();
        let close_qty = *&close_orders.iter().map(|x| x.exec_qty).sum::<f64>();

        let initial_collateral_qty = *&open_order.iter().map(|x| x.exec_qty * x.price).sum::<f64>();
        let initial_fees = *&open_order
            .iter()
            .map(|x| {
                let bybit_order: OrderWS = json_decode(&x.data).unwrap();
                bybit_order.exec_fee
            })
            .sum::<f64>();

        close_orders.sort_by_key(|x| x.timestamp);
        let mut final_collateral = *&close_orders
            .iter()
            .map(|x| x.exec_qty * x.price)
            .sum::<f64>();
        let final_fees = *&close_orders
            .iter()
            .map(|x| {
                let bybit_order: OrderWS = json_decode(&x.data).unwrap();
                bybit_order.exec_fee
            })
            .sum::<f64>();

        if close_qty != open_qty {
            let last_order = close_orders.last().unwrap();
            final_collateral += last_order.price * last_order.leaves_qty;
        }

        pnl.pnl = if open_order.first().unwrap().data.contains("Buy") {
            // LONG DEAL
            final_collateral - initial_collateral_qty - initial_fees - final_fees
        } else {
            // SHORT DEAL
            initial_collateral_qty - final_collateral - initial_fees - final_fees
        };

        pnl.store("signaland_users_pnl").await;
    }

    Ok(json!(format!("DONE!")))
}
