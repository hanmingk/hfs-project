use std::path::{Path, PathBuf};

pub mod auth;
pub mod cd;
pub mod command;
pub mod get;
pub mod ls;
pub mod mkdir;
pub mod mv;
pub mod put;
pub mod rm;

pub fn push_normalize<P: AsRef<Path>>(src: &mut PathBuf, path: P) -> bool {
    let path = path.as_ref();

    if path.is_absolute() {
        src.push(path);
        return true;
    }

    let mut components = path.components();

    while let Some(component) = components.next() {
        let Some(comp) = component.as_os_str().to_str() else {
            continue;
        };

        match comp {
            "." | "" => {}
            ".." => {
                if !src.pop() {
                    return false;
                }
            }
            _ => src.push(comp),
        }
    }

    true
}
