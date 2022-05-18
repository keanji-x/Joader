use tokio::sync::mpsc::{channel, Receiver, Sender};
const DEL_SIG: u32 = u32::MAX;
use num_traits::FromPrimitive;
#[derive(Debug)]
pub struct LoaderSender<T> {
    loader_id: u64,
    sender: Sender<T>,
}

#[derive(Debug)]
pub struct LoaderReceiver<T> {
    loader_id: u64,
    recv: Receiver<T>,
}

pub fn new<T: std::fmt::Debug>(loader_id: u64) -> (LoaderSender<T>, LoaderReceiver<T>) {
    let (sender, recv) = channel::<T>(1048);
    (
        LoaderSender { loader_id, sender },
        LoaderReceiver { loader_id, recv },
    )
}

impl<T: std::fmt::Debug + Copy> LoaderSender<T>
where
    T: FromPrimitive,
{
    pub fn get_loader_id(&self) -> u64 {
        self.loader_id
    }

    pub async fn send(&self, d: T) {
        self.sender.send(d).await.unwrap();
    }

    pub async fn close(&self) {
        self.sender
            .send(T::from_u32(DEL_SIG).unwrap())
            .await
            .unwrap();
    }
}

impl<T> LoaderReceiver<T>
where
    T: FromPrimitive + PartialEq,
{
    pub fn get_loader_id(&self) -> u64 {
        self.loader_id
    }
    pub async fn recv_batch(&mut self, bs: u32) -> (Vec<T>, bool) {
        assert!(bs != 0);
        let del_sig = T::from_u32(DEL_SIG).unwrap();
        let mut ret = Vec::new();
        let mut empty = false;
        for _ in 0..bs {
            let v = self.recv.recv().await.unwrap();
            empty = v == (del_sig);
            ret.push(v);
            if empty {
                ret.pop();
                return (ret, empty);
            }
        }
        (ret, empty)
    }
    pub async fn recv_all(&mut self) -> (Vec<T>, bool) {
        let del_sig = T::from_u32(DEL_SIG).unwrap();
        let mut ret = Vec::new();
        let mut empty = false;
        loop {
            match self.recv.try_recv() {
                Ok(v) => {
                    empty = v == del_sig;
                    ret.push(v)
                }
                Err(_err) => {
                    // assert_eq!(err, TryRecvError::Empty);
                    break;
                }
            }
        }
        if empty {
            ret.pop();
        }
        (ret, empty)
    }

    pub fn close(&mut self) {
        self.recv.close();
    }
}
