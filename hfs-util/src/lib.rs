pub mod wdlock;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[test]
    fn it_works() {
        let mut ve = VecDeque::from([1, 2, 3, 4, 5]);
        ve.pop_back();
        println!("{:?}", ve);
        ve.pop_front();
        println!("{:?}", ve);
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
