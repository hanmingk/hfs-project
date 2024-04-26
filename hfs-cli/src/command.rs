use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "")]
pub enum HFSCommand {
    /// List information about path(the current diretory by default)
    Ls {
        /// target path
        path: Option<String>,

        /// use a long listing format
        #[arg(short)]
        list: bool,
    },
    /// Modify the current working directory
    Cd {
        path: Option<String>,
    },
    /// Remove the file(s)
    Rm {
        /// target path
        path: String,

        /// remove directories and their contents recursively
        #[arg(short, long)]
        recursive: bool,
    },
    /// Create the DIRECTORY(ies), if they do not already exist.
    Mkdir {
        /// target path
        path: String,

        /// no error if existing, make parent directories as needed,
        /// with their file modes unaffected by any -m option.
        #[arg(short, long)]
        parents: bool,
    },
    /// Upload files locally to hfs.
    Put {
        /// local path
        local_path: String,

        /// hfs path
        hfs_path: Option<String>,
    },
    /// Download files from hfs to local
    Get {
        /// hfs path
        hfs_path: String,

        /// local path
        local_path: Option<String>,
    },
    /// Rename SOURCE to DEST, or move SOURCE(s) to DIRECTORY.
    Mv {
        /// src path
        src_path: String,

        /// target path
        target_path: String,
    },
    Useradd {
        username: String,
        passwd: String,
    },
    Passwd {
        username: String,
        passwd: String,
    },
    Userdel {
        username: String,
    },
    Groupadd {
        groupname: String,
    },
    Groupdel {
        groupname: String,
    },
    Exit,
    Clear,
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::push_normalize;

    use super::HFSCommand;

    #[derive(Debug, Parser)]
    #[command(name = "")] // This name will show up in clap's error messages, so it is important to set it to "".
    enum SampleCommand {
        Download {
            path: String,
            /// Some explanation about what this flag do.
            #[arg(long)]
            check_sha: bool,
        },
        /// A command to upload things.
        Upload,
        Login {
            /// Optional. You will be prompted if you don't provide it.
            #[arg(short, long)]
            username: Option<String>,
        },
    }

    #[test]
    fn test() {
        // let cmd_line = "ls -l /home/hmk";
        // let cmd_line = "  --help";

        // let args = cmd_line.split_whitespace().collect::<Vec<&str>>();
        // println!("{:?}", args);

        let args = vec!["", "mkdir", "-h"];

        let res = HFSCommand::try_parse_from(args);

        match res {
            Ok(cmd) => println!("{:?}", cmd),
            Err(err) => err.print().unwrap(),
        }
    }

    #[test]
    fn test1() {
        let mut base_path = PathBuf::from("/home/user");
        push_normalize(&mut base_path, "/document");

        println!("Absolute path: {:?}", base_path);
    }
}
