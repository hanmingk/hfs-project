const SEPARATOR: u8 = b'/';

#[derive(Debug, PartialEq, Eq)]
pub struct HFSPath<'a> {
    src: &'a [u8],
}

impl<'a> From<&'a [u8]> for HFSPath<'a> {
    fn from(value: &'a [u8]) -> HFSPath<'a> {
        HFSPath::from(value)
    }
}

pub struct PathComponents<'a> {
    slice: &'a [u8],
    next_index: usize,
}

impl<'a> Iterator for PathComponents<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index == self.slice.len() + 1 || self.next_index == self.slice.len() {
            return None;
        }

        let start_index = self.next_index;
        while let Some(byte) = self.slice.get(self.next_index) {
            if *byte == SEPARATOR {
                break;
            }
            self.next_index += 1;
        }
        let item = Some(&self.slice[start_index..self.next_index]);

        // Skip over separators.
        loop {
            self.next_index += 1;
            match self.slice.get(self.next_index) {
                Some(byte) => {
                    if *byte != SEPARATOR {
                        break;
                    }
                }
                None => break,
            }
        }

        item
    }
}

impl<'a> HFSPath<'a> {
    pub fn is_root(&self) -> bool {
        for index in 0..self.src.len() {
            if self.src[index] != SEPARATOR {
                return false;
            }
        }
        true
    }

    pub fn from(src: &[u8]) -> HFSPath {
        HFSPath { src }
    }

    pub fn components(&self) -> PathComponents {
        PathComponents {
            slice: self.src,
            next_index: 0,
        }
    }

    pub fn parent_path(&self) -> Option<HFSPath> {
        self.parent_path_index()
            .map(|index| HFSPath::from(&self.src[..index]))
    }

    pub fn parent_path_ref(&self) -> Option<&[u8]> {
        self.parent_path_index().map(|index| &self.src[..index])
    }

    pub fn last_path_name(&self) -> Option<&[u8]> {
        if self.is_root() {
            return Some(b"/");
        }
        let path_components = self.components();
        path_components.last()
    }

    pub fn as_ref(&self) -> &[u8] {
        self.src
    }

    fn parent_path_index(&self) -> Option<usize> {
        if self.is_root() || self.src.is_empty() {
            return None;
        }

        let mut index_end = self.src.len();
        for index in (1..index_end).rev() {
            unsafe {
                if *self.src.get_unchecked(index) != SEPARATOR
                    && *self.src.get_unchecked(index - 1) == SEPARATOR
                {
                    index_end = index - 1;
                    break;
                }
            }
        }

        // Skip over separators.
        while let Some(byte) = self.src.get(index_end) {
            if *byte != SEPARATOR || index_end == 0 {
                break;
            }
            index_end -= 1;
        }

        Some(index_end + 1)
    }
}

#[cfg(test)]
mod test {
    // use std::path::Path;

    use super::HFSPath;

    #[test]
    fn test() {
        // let path = Path::new("/home/hmk");
        let hfs_path = HFSPath::from(b"/home/hmk");
        let mut path_components = hfs_path.components();
        assert_eq!(Some(&b""[..]), path_components.next());
        assert_eq!(Some(&b"home"[..]), path_components.next());
        assert_eq!(Some(&b"hmk"[..]), path_components.next());
        assert_eq!(None, path_components.next());

        let hfs_path = HFSPath::from(b"/home/hmk/");
        let mut path_components = hfs_path.components();
        assert_eq!(Some(&b""[..]), path_components.next());
        assert_eq!(Some(&b"home"[..]), path_components.next());
        assert_eq!(Some(&b"hmk"[..]), path_components.next());
        assert_eq!(None, path_components.next());

        let hfs_path = HFSPath::from(b"/home/a/");
        let mut path_components = hfs_path.components();
        assert_eq!(Some(&b""[..]), path_components.next());
        assert_eq!(Some(&b"home"[..]), path_components.next());
        assert_eq!(Some(&b"a"[..]), path_components.next());
        assert_eq!(None, path_components.next());

        let hfs_path = HFSPath::from(b"/");
        let mut path_components = hfs_path.components();
        assert_eq!(Some(&b""[..]), path_components.next());
        assert_eq!(None, path_components.next());

        let hfs_path = HFSPath::from(b"/");
        let parent_path = hfs_path.parent_path_ref();
        assert_eq!(None, parent_path);
        let hfs_path = HFSPath::from(b"/home");
        let parent_path = hfs_path.parent_path_ref();
        assert_eq!(Some(&b"/"[..]), parent_path);
        let hfs_path = HFSPath::from(b"/home//hmk//");
        let parent_path = hfs_path.parent_path_ref();
        assert_eq!(Some(&b"/home"[..]), parent_path);
    }
}
