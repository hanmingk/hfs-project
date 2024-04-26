pub trait BinarySearch {
    type Key: Ord + ?Sized;
    fn get_key(&self) -> &Self::Key;
}

// (true, index) if the child with this name already exists;
// otherwise, return (false, index) that index is insertion point;
pub fn binary_search<T: BinarySearch>(list: &[T], key: &<T as BinarySearch>::Key) -> (bool, usize) {
    if list.len() == 0 {
        return (false, 0);
    }

    // Binary search
    let mut low = 0;
    let mut high = list.len() - 1;

    while low <= high {
        let mid = (low + high) >> 1;
        match unsafe { key.cmp(list.get_unchecked(mid).get_key()) } {
            std::cmp::Ordering::Less => {
                if mid == 0 {
                    return (false, mid);
                }
                high = mid - 1;
            }
            std::cmp::Ordering::Equal => return (true, mid),
            std::cmp::Ordering::Greater => low = mid + 1,
        }
    }

    (false, low)
}
