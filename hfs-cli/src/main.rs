use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::{Arg, Command, Parser};
use faststr::FastStr;
use hfs_cli::{
    auth::{group_add, group_remove, user_add, user_remove},
    cd::hfs_cd,
    command::HFSCommand,
    get::hfs_get,
    ls::hfs_ls,
    mkdir::hfs_mkdir,
    mv::hfs_mv,
    push_normalize,
    put::hfs_put,
    rm::hfs_rm,
};
use hfs_proto::master::{KeepHeartbeatRequest, SignInRequest};
use lazy_static::lazy_static;
use rustyline::{error::ReadlineError, history::FileHistory, DefaultEditor, Editor, Result};
use tokio::time::{Duration, Instant};

lazy_static! {
    static ref AUTH_CLIENT: hfs_proto::master::AuthClient = {
        let addr: SocketAddr = "[::1]:4090".parse().unwrap();
        hfs_proto::master::AuthClientBuilder::new("auth client")
            .address(addr)
            .build()
    };
    static ref FS_CLIENT: hfs_proto::master::FileSystemClient = {
        let addr: SocketAddr = "[::1]:4090".parse().unwrap();
        hfs_proto::master::FileSystemClientBuilder::new("auth client")
            .address(addr)
            .build()
    };
}

#[derive(Debug)]
struct CliInfo {
    #[allow(dead_code)]
    username: FastStr,
    #[allow(dead_code)]
    passwd: FastStr,
    token: FastStr,
    hfs_path: FastStr,
    local_path: FastStr,
    input_time: Arc<Mutex<Instant>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // let matches = command!()
    let matches = Command::new("hfs-cli")
        // Application configuration
        .version("1.0.0")
        .author("hmk")
        .about("HFS Cli")
        .arg(Arg::new("user").short('u').long("user").required(true))
        .arg(Arg::new("passwd").short('p').long("passwd").required(true))
        .get_matches();

    let Some(username) = matches
        .get_one::<String>("user")
        .map(|v| FastStr::from_string(v.clone()))
    else {
        return Ok(());
    };
    let Some(passwd) = matches
        .get_one::<String>("passwd")
        .map(|v| FastStr::from_string(v.clone()))
    else {
        return Ok(());
    };

    let Ok(local_path) =
        std::env::current_dir().map(|p| FastStr::from_string(p.to_str().unwrap().to_string()))
    else {
        return Ok(());
    };

    let Some(token) = get_token(&username, &passwd).await else {
        return Ok(());
    };

    let hfs_path = if username == "root" {
        String::from("/").into()
    } else {
        format!("/home/{}", username).into()
    };
    let mut cli_info = CliInfo {
        username,
        passwd,
        token,
        hfs_path,
        local_path,
        input_time: Arc::new(Mutex::new(Instant::now())),
    };

    // heartbeat background task
    tokio::spawn(heartbeat_task(cli_info.token.clone()));

    // live background task
    tokio::spawn(live_task(cli_info.input_time.clone()));

    // `()` can be used when no completer is required
    let mut rl = DefaultEditor::new()?;

    let mut is_shutdown = false;

    while !is_shutdown {
        let prompt = format!("{}$ ", cli_info.hfs_path);
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                if let Some(cmd) = read_command(line) {
                    cmd_execute(&mut cli_info, cmd, &mut rl, &mut is_shutdown).await;
                }
            }
            Err(ReadlineError::Interrupted) => {
                is_shutdown = true;
            }
            Err(ReadlineError::Eof) => {
                is_shutdown = true;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                is_shutdown = true;
            }
        }
    }

    println!("bey bey!!");

    Ok(())
}

async fn cmd_execute(
    cli_info: &mut CliInfo,
    cmd: HFSCommand,
    rl: &mut Editor<(), FileHistory>,
    is_shutdown: &mut bool,
) {
    {
        let mut input_time = cli_info.input_time.lock().unwrap();
        *input_time = Instant::now();
    }

    match cmd {
        HFSCommand::Ls { path, list } => {
            hfs_ls(
                &FS_CLIENT,
                path.map(FastStr::from_string)
                    .unwrap_or(cli_info.hfs_path.clone()),
                list,
            )
            .await
        }
        HFSCommand::Cd { path } => {
            if let Some(path) = path {
                let mut cur_path = PathBuf::from(cli_info.hfs_path.as_str());
                push_normalize(&mut cur_path, path);

                let target_path: FastStr = cur_path.to_str().unwrap().to_string().into();

                if hfs_cd(&FS_CLIENT, target_path.clone()).await {
                    cli_info.hfs_path = target_path;
                } else {
                    println!("no such file or directory");
                }
            }
        }
        HFSCommand::Rm { path, recursive } => {
            let mut rm_path = PathBuf::from(cli_info.hfs_path.as_str());
            push_normalize(&mut rm_path, path);

            let rm_path: FastStr = rm_path.to_str().unwrap().to_string().into();

            if let Err(msg) = hfs_rm(&FS_CLIENT, cli_info.token.clone(), rm_path, recursive).await {
                println!("Rm error: {}", msg);
            }
        }
        HFSCommand::Mkdir { path, parents } => {
            let mut cur_path = PathBuf::from(cli_info.hfs_path.as_str());
            push_normalize(&mut cur_path, path);

            let target_path: FastStr = cur_path.to_str().unwrap().to_string().into();

            hfs_mkdir(&FS_CLIENT, cli_info.token.clone(), target_path, parents).await;
        }
        HFSCommand::Put {
            local_path,
            hfs_path,
        } => {
            let mut path1 = PathBuf::from(cli_info.local_path.as_str());
            push_normalize(&mut path1, local_path);

            let mut path2 = PathBuf::from(cli_info.hfs_path.as_str());
            if let Some(hfs_path) = hfs_path {
                push_normalize(&mut path2, hfs_path);
            }

            let hfs_path: FastStr = path2.to_str().unwrap().to_string().into();

            println!(
                "put {:?} => {:?}\nWaiting for data to be uploaded ...",
                path1, path2
            );

            match hfs_put(&FS_CLIENT, cli_info.token.clone(), path1, hfs_path).await {
                Ok(_) => println!("File put success!!"),
                Err(msg) => println!("Put error: {}", msg),
            }
        }
        HFSCommand::Get {
            hfs_path,
            local_path,
        } => {
            let mut path1 = PathBuf::from(cli_info.local_path.as_str());
            if let Some(local_path) = local_path {
                push_normalize(&mut path1, local_path);
            }

            let mut path2 = PathBuf::from(cli_info.hfs_path.as_str());
            push_normalize(&mut path2, hfs_path);

            let hfs_path: FastStr = path2.to_str().unwrap().to_string().into();

            println!(
                "get {:?} => {:?}\nWaiting for file to be download ...",
                path2, path1
            );

            match hfs_get(&FS_CLIENT, cli_info.token.clone(), path1, hfs_path).await {
                Ok(_) => println!("File get success!!"),
                Err(err_msg) => println!("File get error: {}", err_msg),
            }
        }
        HFSCommand::Mv {
            src_path,
            target_path,
        } => {
            let mut cur_path = PathBuf::from(cli_info.hfs_path.as_str());
            push_normalize(&mut cur_path, src_path);

            let src_path: FastStr = cur_path.as_os_str().to_str().unwrap().to_string().into();

            let mut cur_path = PathBuf::from(cli_info.hfs_path.as_str());
            push_normalize(&mut cur_path, target_path);

            let target_path: FastStr = cur_path.as_os_str().to_str().unwrap().to_string().into();

            hfs_mv(&FS_CLIENT, cli_info.token.clone(), src_path, target_path).await;
        }
        HFSCommand::Useradd { username, passwd } => {
            user_add(
                &AUTH_CLIENT,
                cli_info.token.clone(),
                username.into(),
                passwd.into(),
            )
            .await
        }
        HFSCommand::Passwd {
            username: _,
            passwd: _,
        } => {}
        HFSCommand::Userdel { username } => {
            user_remove(&AUTH_CLIENT, cli_info.token.clone(), username.into()).await
        }
        HFSCommand::Groupadd { groupname } => {
            group_add(&AUTH_CLIENT, cli_info.token.clone(), groupname.into()).await
        }
        HFSCommand::Groupdel { groupname } => {
            group_remove(&AUTH_CLIENT, cli_info.token.clone(), groupname.into()).await
        }
        HFSCommand::Exit => *is_shutdown = true,
        HFSCommand::Clear => {
            if let Err(err) = rl.clear_screen() {
                println!("clear screen error: {}", err.to_string());
            }
        }
    }
}

fn read_command(line: String) -> Option<HFSCommand> {
    if line.trim().is_empty() {
        return None;
    }

    match HFSCommand::try_parse_from(std::iter::once("").chain(line.split_whitespace())) {
        Ok(cmd) => Some(cmd),
        Err(err) => {
            err.print().unwrap();
            None
        }
    }
}

async fn get_token(username: &FastStr, passwd: &FastStr) -> Option<FastStr> {
    match AUTH_CLIENT
        .sign_in(SignInRequest {
            username: username.clone(),
            passwd: passwd.clone(),
        })
        .await
    {
        Ok(resp) => Some(resp.into_inner().token),
        Err(err) => {
            println!("Sign error: {}", err.message());
            None
        }
    }
}

async fn heartbeat_task(token: FastStr) {
    const INTERVAL_TIME: Duration = Duration::from_secs(25);

    loop {
        if let Err(err) = AUTH_CLIENT
            .keep_heartbeat(KeepHeartbeatRequest {
                token: token.clone(),
            })
            .await
        {
            println!("keep heartbeat error: {}", err.message());
            println!("Remote connection disconnected\nbey bey!!");
            std::process::exit(0);
        }

        tokio::time::sleep_until(Instant::now() + INTERVAL_TIME).await;
    }
}

async fn live_task(input_time: Arc<Mutex<Instant>>) {
    const INTERVAL_TIME: Duration = Duration::from_secs(120);

    loop {
        let current_time = Instant::now();
        {
            let time = input_time.lock().unwrap();
            if current_time.duration_since(*time) > INTERVAL_TIME {
                println!("\nRemote connection disconnected\nbey bey!!");
                std::process::exit(0);
            }
        }

        tokio::time::sleep_until(current_time + INTERVAL_TIME).await;
    }
}
