use chrono::{TimeZone, Utc};
use faststr::FastStr;
use hfs_proto::master::{FileSystemClient, ListDirectoryRequest, OPPermissions};

pub async fn hfs_ls(fs_client: &FileSystemClient, path: FastStr, list: bool) {
    match fs_client
        .list_directory(ListDirectoryRequest { path })
        .await
    {
        Ok(resp) => {
            let list_item = resp.into_inner().items;

            if list {
                println!("total {}", list_item.len());

                let mut owner_with = 0;
                let mut group_with = 0;
                for item in list_item.iter() {
                    if item.owner.len() > owner_with {
                        owner_with = item.owner.len();
                    }

                    if item.group.len() > group_with {
                        group_with = item.group.len();
                    }
                }

                for item in list_item {
                    let mut perms = String::from("-------");
                    let bytes = unsafe { perms.as_bytes_mut() };
                    if item.is_dir {
                        bytes[0] = b'd';
                    }

                    for it in item.perms.iter().enumerate() {
                        let index = it.0 * 2 + 1;
                        match it.1 {
                            OPPermissions::ReadOnly => bytes[index] = b'r',
                            OPPermissions::WriteOnly => bytes[index + 1] = b'w',
                            OPPermissions::ReadWithWrite => {
                                bytes[index] = b'r';
                                bytes[index + 1] = b'w';
                            }
                            OPPermissions::NoPermissions => {}
                        }
                    }

                    let dt = Utc.timestamp_opt(item.update_time as i64, 0).unwrap();

                    let formatted_time = dt.format("%b %e %H:%M").to_string();

                    println!(
                        "{} {:<ow$} {:<gw$} {} {}",
                        perms,
                        item.owner,
                        item.group,
                        formatted_time,
                        item.name,
                        ow = owner_with,
                        gw = group_with
                    );
                }
            } else {
                let mut display_string = String::from("");
                for item in list_item {
                    display_string.push_str(&item.name);
                    display_string.push_str("  ");
                }

                println!("{display_string}");
            }
        }
        Err(err) => println!("{}", err.message()),
    }
}
