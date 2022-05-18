use crate::joader::joader_table::JoaderTable;
use crate::proto::dataset::dataset_svc_server::DatasetSvc;
use crate::proto::dataset::*;
use crate::proto::distributed::distributed_svc_client::DistributedSvcClient;
use crate::proto::distributed::RegisterDatasetRequest;
use crate::Role;
use crate::{dataset, joader::joader::Joader};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

use super::GlobalID;
#[derive(Debug)]
pub struct DatasetSvcImpl {
    joader_table: Arc<Mutex<JoaderTable>>,
    dataset_table: Arc<Mutex<HashMap<String, u32>>>,
    id: GlobalID,
    followers: Vec<String>,
    role: Role,
}

impl DatasetSvcImpl {
    pub fn new(
        joader_table: Arc<Mutex<JoaderTable>>,
        dataset_table: Arc<Mutex<HashMap<String, u32>>>,
        id: GlobalID,
        followers: Vec<String>,
        role: Role,
    ) -> DatasetSvcImpl {
        Self {
            joader_table,
            dataset_table,
            id,
            followers,
            role,
        }
    }
}

#[async_trait]
impl DatasetSvc for DatasetSvcImpl {
    async fn create_dataset(
        &self,
        request: Request<CreateDatasetRequest>,
    ) -> Result<Response<CreateDatasetResponse>, Status> {
        let request = request.into_inner();
        let mut jt = self.joader_table.lock().await;
        let mut dt = self.dataset_table.lock().await;
        if dt.contains_key(&request.name) {
            return Err(Status::already_exists(format!(
                "{:?} has already existed",
                request
            )));
        }

        log::debug!("Create dataset {:?}", request);
        let id = self.id.get_dataset_id().await;
        dt.insert(request.name.clone(), id);
        // insert dataset to dataset table
        let joader = Joader::new(dataset::build_dataset(request.clone(), id));
        jt.add_joader(joader);
        if self.role == Role::Leader {
            for ip_port in self.followers.iter().cloned() {
                let mut f = DistributedSvcClient::connect(ip_port.to_string())
                    .await
                    .unwrap();
                let r = RegisterDatasetRequest {
                    request: Some(request.clone()),
                    dataset_id: id,
                };
                f.register_dataset(r).await?;
            }
        }

        Ok(Response::new(CreateDatasetResponse { status: None }))
    }

    async fn delete_dataset(
        &self,
        request: Request<DeleteDatasetRequest>,
    ) -> Result<Response<DeleteDatasetResponse>, Status> {
        log::debug!("call delete dataset {:?}", request);
        let request = request.into_inner();
        let mut jt = self.joader_table.lock().await;
        let mut dt = self.dataset_table.lock().await;
        match dt.get(&request.name) {
            Some(id) => {
                jt.del_joader(*id);
                dt.remove(&request.name);
                Ok(Response::new(DeleteDatasetResponse { status: None }))
            }
            None => Err(Status::not_found(format!("{:?} not found", request))),
        }
    }
}
