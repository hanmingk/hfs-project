use std::io::SeekFrom;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, Result};

#[tokio::main]
async fn main() -> Result<()> {
    println!("1");
    // 打开文件
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("/home/hmk/code/rust/hfs-project/test_data/test_bat.txt")
        .await?;

    println!("2");
    // 读取文件内容
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await?;

    // 修改文件内容（这里简单地将文件内容转换为大写）
    contents = contents.iter().map(|&b| b.to_ascii_lowercase()).collect();

    println!("3");
    // 将修改后的数据写入文件
    file.seek(SeekFrom::Start(3)).await?;
    file.write_all(&contents).await?;

    println!("4");

    Ok(())
}
