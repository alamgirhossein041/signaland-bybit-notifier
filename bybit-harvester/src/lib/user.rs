pub mod user {

    use chrono::{DateTime, Utc};
    use serde::{Serialize, Deserialize};
    use serde_json::Value;
    use std::{collections::HashMap, hash::Hash};
    use serde_dynamo::from_items;

    use signaland_rust_lib::signaland::{services::aws::dynamo::{
        DyVariables, DyNames, string_into_av, i64_into_av, query,
        column_as_f64, column_as_string, generate_put_insert,
        insert_rows, column_as_i64, TableCandles
    }, strategies::Strategies};

    const FUNCTION_NAME_PREFIX: &'static str = "signaland-user-executor-";
    const USERS_TABLE_NAME: &'static str = "signaland_users";

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct User {
        pub id: String,
        pub created_at: String,
        pub data: Value,
        pub email: String,
        #[serde(default)]
        pub free_subscription: bool,
        pub payment_address: String,
        pub settings: UserSettings,
        pub subscriptions: Vec<String>,
        pub subscription_expires_on: String,
        #[serde(default)]
        pub subscription_type: String,
        #[serde(default)]
        pub telegram_account: String,
        pub updated_at: String,
        pub verified: bool
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct UserSettings {
        pub streams: Vec<UserStream>
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct UserStream {
        pub id: f64,
        pub created_at: String,
        pub active: bool,
        pub description: String,
        pub exchange: String,
        pub opt: OptValues,
        #[serde(default)]
        pub pairs: Value,
        pub receivers: StreamReceivers,
        pub strategy: String,
        pub title: String
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct OptValues {
        #[serde(default)]
        pub children: bool,
        #[serde(default)]
        pub leverage: f32,
        #[serde(default)]
        pub order_size: f32,
        #[serde(default)]
        pub pyramiding: usize
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct StreamReceivers {
        #[serde(rename(deserialize = "3commas"))]
        #[serde(default)]
        pub threecommas: ThreeCommasReceiver,
        #[serde(default)]
        pub telegram: String,
        #[serde(default)]
        pub bybit: ByBitReceiver 
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct ThreeCommasReceiver {
        bot_id: i64,
        email_token: String
    }

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct ByBitReceiver {
        pub api_key: String,
        pub api_secret: String
    }

    impl User {
        pub fn get_streams(&self, strategy: Strategies) -> Vec<UserStream> {
            self.settings.streams
                .clone()
                .into_iter()
                .filter(|x| x.strategy == strategy.to_str())
                .collect()
        }

        pub fn has_subscription_expired(&self) -> bool {
            self.subscription_expires_on.parse::<DateTime<Utc>>().unwrap() < Utc::now()
        }
    }

    pub async fn get_all_pro_users() -> Result<Vec<User>, &'static str> {

        // Pro Query Output

        let ask: &str = "subscription_type = :subtype";

        let vars: DyVariables = HashMap::from([ (":subtype".to_string(), string_into_av("pro")) ]);

        let pro_query_output = query(
            USERS_TABLE_NAME,
            ask,
            vars,
            None,
            None,
            Some(String::from("subscription_type-created_at-index"))
        ).await;

        let pro_res = pro_query_output.items;
        if pro_res.is_none() { return Err("Error while querying for pro users ") }

        let pro_users: Vec<User> = from_items(pro_res.unwrap()).unwrap();

        // Pro Year Query Output

        let ask: &str = "subscription_type = :subtype";

        let vars: DyVariables = HashMap::from([ (":subtype".to_string(), string_into_av("pro_year")) ]);

        let proyear_query_output = query(
            USERS_TABLE_NAME,
            ask,
            vars,
            None,
            None,
            Some(String::from("subscription_type-created_at-index"))
        ).await;

        let proyear_res = proyear_query_output.items;
        if proyear_res.is_none() { return Err("Error while querying for pro users ") }

        let pro_year_users = from_items(proyear_res.unwrap()).unwrap();
        
        Ok([pro_users, pro_year_users].concat())
    }

    pub async fn get_users_bybit_streams() -> HashMap<(String, String), (String, String)> {
        let users = get_all_pro_users().await.unwrap_or(vec![]);
        let mut result: HashMap<(String, String), (String, String)> = HashMap::new();

        for user in users {
            for stream in user.settings.streams {
                if stream.exchange != "BYBIT" { continue; }
                result.insert((user.id.clone(), format!("{}", stream.id)), (stream.receivers.bybit.api_key.clone(), stream.receivers.bybit.api_secret.clone()));
            }
        }

        result
    }

}