use crate::new_dataset::build_dataset;
use crate::new_joader::joader::Joader;
use crate::new_joader::joader_table::JoaderTable;
use crate::proto::dataset::dataset_svc_server::DatasetSvc;
use crate::proto::dataset::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

use super::{IDTable, IdGenerator};
#[derive(Debug)]
pub struct DatasetSvcImpl {
    joader_table: Arc<Mutex<JoaderTable>>,
    dataset_id_table: IDTable,
    id_gen: IdGenerator,
}

impl DatasetSvcImpl {
    pub fn new(
        joader_table: Arc<Mutex<JoaderTable>>,
        dataset_id_table: IDTable,
        id_gen: IdGenerator,
    ) -> DatasetSvcImpl {
        Self {
            joader_table,
            dataset_id_table,
            id_gen,
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
        let mut dt = self.dataset_id_table.lock().await;
        if dt.contains_key(&request.name) {
            return Err(Status::already_exists(format!(
                "{:?} has already existed",
                request
            )));
        }

        log::debug!("Create dataset {:?}", request);
        let id = self.id_gen.get_dataset_id();
        dt.insert(request.name.clone(), id);
        // insert dataset to dataset table
        let joader = Joader::new(build_dataset(request.clone(), id));
        jt.add_joader(joader);
        Ok(Response::new(CreateDatasetResponse { status: None }))
    }

    async fn delete_dataset(
        &self,
        request: Request<DeleteDatasetRequest>,
    ) -> Result<Response<DeleteDatasetResponse>, Status> {
        log::debug!("call delete dataset {:?}", request);
        let request = request.into_inner();
        let mut jt = self.joader_table.lock().await;
        let mut dt = self.dataset_id_table.lock().await;
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
