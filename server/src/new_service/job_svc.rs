use super::{IDTable, IdGenerator};
use crate::job::Job;
use crate::new_joader::joader_table::JoaderTable;
use crate::proto::job::job_svc_server::JobSvc;
use crate::proto::job::Data;
use crate::proto::job::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

#[derive(Debug)]
pub struct JobSvcImpl {
    id_gen: IdGenerator,
    joader_table: Arc<Mutex<JoaderTable>>,
    job_id_table: IDTable,
    dataset_id_table: IDTable,
    recv_table: Arc<Mutex<HashMap<u64, Receiver<Arc<Vec<Data>>>>>>,
}

impl JobSvcImpl {
    pub fn new(
        joader_table: Arc<Mutex<JoaderTable>>,
        id_gen: IdGenerator,
        job_id_table: IDTable,
        dataset_id_table: IDTable,
    ) -> Self {
        Self {
            joader_table,
            recv_table: Default::default(),
            job_id_table,
            id_gen,
            dataset_id_table,
        }
    }
}

#[async_trait]
impl JobSvc for JobSvcImpl {
    async fn create_job(
        &self,
        request: Request<CreateJobRequest>,
    ) -> Result<Response<CreateJobResponse>, Status> {
        let request = request.into_inner();
        let mut rt = self.recv_table.lock().await;
        let mut jt = self.joader_table.lock().await;
        let mut job_id_table = self.job_id_table.lock().await;
        let dt = self.dataset_id_table.lock().await;
        log::info!("call create loader {:?}", request);
        let dataset_id = *dt
            .get(&request.dataset_name)
            .ok_or_else(|| Status::not_found(&request.dataset_name))?;
        let joader = jt.get_mut(dataset_id);

        let job_id = self.id_gen.get_job_id();
        let (job, r) = Job::new(job_id);
        joader.add_job(job).await;
        rt.insert(job_id, r);
        job_id_table.insert(request.name.clone(), job_id);
        Ok(Response::new(CreateJobResponse {
            length: joader.len() as u64,
            job_id,
        }))
    }

    async fn next(&self, request: Request<NextRequest>) -> Result<Response<NextResponse>, Status> {
        let request = request.into_inner();
        let loader_id = request.job_id;
        let mut rt = self.recv_table.lock().await;

        let recv = rt
            .get_mut(&loader_id)
            .ok_or_else(|| Status::not_found(format!("Loader {} not found", loader_id)))?;
        let data = recv.recv().await;
        match data {
            Some(data) => Ok(Response::new(NextResponse {
                data: (*data).clone(),
            })),
            None => Ok(Response::new(NextResponse { data: Vec::new() })),
        }
    }

    async fn delete_job(
        &self,
        request: Request<DeleteJobRequest>,
    ) -> Result<Response<DeleteJobResponse>, Status> {
        log::info!("call delete loader {:?}", request);
        let request = request.into_inner();
        let mut rt = self.recv_table.lock().await;
        let mut jt = self.joader_table.lock().await;
        let job_id_table = self.job_id_table.lock().await;
        let dataset_id_table = self.dataset_id_table.lock().await;
        let job_id = job_id_table[&request.name];
        let dataset_id = dataset_id_table[&request.dataset_name];
        // 1 remove loader
        let joader = jt.get_mut(dataset_id);
        joader.del_job(job_id).await;
        // 2 remove recv table
        rt.remove(&job_id);
        Ok(Response::new(DeleteJobResponse {}))
    }
}
