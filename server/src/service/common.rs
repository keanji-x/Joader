use std::sync::Arc;

use tokio::sync::Mutex;

use crate::proto::common::{status::Code as RspCode, Status as RspStatus};

pub fn to_status<T>(result: &Result<T, String>) -> RspStatus {
    match result {
        Err(msg) => RspStatus {
            code: RspCode::Err as i32,
            msg: msg.to_string(),
        },
        Ok(_) => RspStatus {
            code: RspCode::Ok as i32,
            msg: "Success".into(),
        },
    }
}

pub fn encode_addr_read_off(addr: u64, off: usize) -> u64 {
    ((off as u64) << 32) + addr
}

pub fn decode_addr_read_off(addr: u64) -> (u64, u32) {
    let read_off = (addr >> 32) as u32;
    (((addr << 32) >> 32), read_off)
}

#[inline]
pub fn succ() -> RspStatus {
    RspStatus {
        code: RspCode::Ok as i32,
        msg: "Success".into(),
    }
}

#[derive(Debug, Clone)]
pub struct GlobalID {
    dataset_id: Arc<Mutex<u32>>,
    loader_id: Arc<Mutex<u32>>,
    host_id: Arc<Mutex<u32>>,
}

impl GlobalID {
    pub async fn get_dataset_id(&self) -> u32 {
        let mut id = self.dataset_id.lock().await;
        let dataset_id = *id;
        *id += 1;
        dataset_id
    }

    pub async fn get_loader_id(&self, dataset_id: u32) -> u64 {
        let mut id = self.loader_id.lock().await;
        let loader_id = *id as u64;
        *id += 1;
        ((dataset_id as u64) << 32) + loader_id
    }

    pub async fn get_host_id(&self) -> u32 {
        let mut id = self.host_id.lock().await;
        let host_id = *id;
        *id += 1;
        host_id
    }

    pub fn new() -> GlobalID {
        GlobalID {
            dataset_id: Arc::new(Mutex::new(0)),
            loader_id: Arc::new(Mutex::new(0)),
            host_id: Arc::new(Mutex::new(0)),
        }
    }

    pub fn parse_dataset_id(loader_id: u64) -> u32 {
        (loader_id >> 32) as u32
    }
}
