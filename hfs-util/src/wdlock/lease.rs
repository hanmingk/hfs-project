#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseID(u64);

impl LeaseID {
    pub fn new(value: u64) -> LeaseID {
        LeaseID(value)
    }
}

impl From<u64> for LeaseID {
    fn from(value: u64) -> Self {
        LeaseID(value)
    }
}

pub trait Lease {
    fn inner_key(&self) -> String;
}
