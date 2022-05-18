use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::proto::job::expr;
use crate::proto::job::Condition;
#[derive(Debug, Clone)]
pub struct IdGenerator {
    dataset_id: Arc<AtomicU64>,
    job_id: Arc<AtomicU64>,
}

impl IdGenerator {
    pub fn get_dataset_id(&self) -> u64 {
        self.dataset_id.fetch_add(1, Ordering::SeqCst);
        self.dataset_id.load(Ordering::SeqCst)
    }

    pub fn get_job_id(&self) -> u64 {
        self.job_id.fetch_add(1, Ordering::SeqCst);
        self.job_id.load(Ordering::SeqCst)
    }

    pub fn new() -> Self {
        Self {
            dataset_id: Arc::new(AtomicU64::new(0)),
            job_id: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Condition {
    pub fn eval(&self, lhs: &str) -> bool {
        let mut res = true;
        for expr in self.exprs.iter() {
            res &= match expr.op {
                op if op == expr::Operation::Lt as i32 => {
                    lhs.parse::<u32>().unwrap() < expr.rhs.as_str().parse::<u32>().unwrap()
                }
                op if op == expr::Operation::Leq as i32 => {
                    lhs.parse::<u32>().unwrap() <= expr.rhs.as_str().parse::<u32>().unwrap()
                }
                op if op == expr::Operation::Gt as i32 => {
                    lhs.parse::<u32>().unwrap() > expr.rhs.as_str().parse::<u32>().unwrap()
                }
                op if op == expr::Operation::Geq as i32 => {
                    lhs.parse::<u32>().unwrap() >= expr.rhs.as_str().parse::<u32>().unwrap()
                }
                op if op == expr::Operation::Eq as i32 => {
                    lhs.parse::<u32>().unwrap() == expr.rhs.as_str().parse::<u32>().unwrap()
                }
                _ => panic!("error op {:?}", expr.op),
            };
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::job::{expr, Condition, Expr};
    #[test]
    fn test_complie() {
        let cond = Condition {
            exprs: vec![
                Expr {
                    op: expr::Operation::Geq as i32,
                    rhs: "0".to_string(),
                },
                Expr {
                    op: expr::Operation::Lt as i32,
                    rhs: "16".to_string(),
                },
            ],
        };
        let vec = (0..128u32).map(|x| x.to_string()).collect::<Vec<_>>();
        let target = (0..16u32).map(|x| x.to_string()).collect::<Vec<_>>();
        let res = vec
            .iter()
            .cloned()
            .filter(|s| cond.eval(s.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(res, target);
        // assert!(cond.eval("0"));
        // assert!(cond.eval("1") == false);
    }
}
