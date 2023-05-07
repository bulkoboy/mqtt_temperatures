use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TSensor{
    pub time: u64,
    pub r#type: String,
    pub sensor: String,
    pub rssi: i32,
    pub battery:f32,
    pub fvalue: String,
    pub value: f32 
}

