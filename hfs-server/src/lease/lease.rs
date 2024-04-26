use std::collections::VecDeque;

use volo::FastStr;

#[derive(Debug)]
pub struct Lease {
    pub(super) expiration_time: u64,
}

impl Lease {
    pub fn new(expiration_time: u64) -> Lease {
        Lease { expiration_time }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseItem(String);

impl From<FastStr> for LeaseItem {
    fn from(value: FastStr) -> Self {
        LeaseItem(value.to_string())
    }
}

impl LeaseItem {
    pub fn new(prefix: &str, source_id: i64) -> LeaseItem {
        let mut bytes = Vec::from(prefix);
        bytes.extend_from_slice(&source_id.to_le_bytes());
        LeaseItem(format!("{}{}", prefix, source_id))
    }
}

#[derive(Debug, Eq)]
pub struct LeaseWithTime {
    pub(super) id: i64,
    pub(super) time: u64,
}

impl Ord for LeaseWithTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        }
    }
}

impl PartialOrd for LeaseWithTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LeaseWithTime {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

#[derive(Debug)]
pub struct LeaseItemGurad {
    pub(super) waiter: VecDeque<i64>,
}

impl LeaseItemGurad {
    pub fn with_lease_id(lease_id: i64) -> LeaseItemGurad {
        LeaseItemGurad {
            waiter: VecDeque::from([lease_id]),
        }
    }

    pub fn _acquire(&mut self, lease_id: &i64) -> Option<i64> {
        self.try_acquire_index(lease_id)
            .map(|index| *self.waiter.get(index).unwrap())
    }

    /// if return Some(LeaseID), Listening lease id
    /// if return None, get lock success
    pub fn _try_acquire(&mut self, lease_id: &i64) -> Option<i64> {
        self.try_acquire_index(lease_id).map(|index| {
            let watch_lease_id = self.waiter.get(index).unwrap().clone();
            self.waiter.remove(index + 1);
            watch_lease_id
        })
    }

    pub fn try_acquire_index(&mut self, lease_id: &i64) -> Option<usize> {
        match self
            .waiter
            .iter()
            .position(|wait_lease_id| wait_lease_id == lease_id)
        {
            Some(index) => {
                if index == 0 {
                    None
                } else {
                    Some(index - 1)
                }
            }
            None => {
                if self.waiter.is_empty() {
                    self.waiter.push_back(*lease_id);
                    None
                } else {
                    self.waiter.push_back(*lease_id);
                    Some(self.waiter.len() - 2)
                }
            }
        }
    }
}
