use tokio::sync::mpsc::{channel, Receiver, Sender};
#[derive(Debug)]
pub struct JobSender<T> {
    job_id: u64,
    sender: Sender<T>,
}

#[derive(Debug)]
pub struct JobReceiver<T> {
    job_id: u64,
    recv: Receiver<T>,
}

// TODO[xj]: 1048 should be fixed
pub fn new<T: std::fmt::Debug>(job_id: u64) -> (JobSender<T>, JobReceiver<T>) {
    let (sender, recv) = channel::<T>(1048);
    (JobSender { job_id, sender }, JobReceiver { job_id, recv })
}

impl<T: std::fmt::Debug> JobSender<T> {
    pub fn get_job_id(&self) -> u64 {
        self.job_id
    }

    pub async fn send(&self, d: T) {
        self.sender.send(d).await.unwrap();
    }
}

impl<T> JobReceiver<T> {
    pub fn get_job_id(&self) -> u64 {
        self.job_id
    }

    pub async fn recv_batch(&mut self, bs: u32) -> Vec<T> {
        assert!(bs != 0);
        let mut ret = Vec::new();
        for _ in 0..bs {
            let v = self.recv.recv().await.unwrap();
            ret.push(v);
        }
        ret
    }

    pub fn close(&mut self) {
        self.recv.close();
    }
}
