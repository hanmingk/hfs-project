use hfs_util::wdlock::server::{DLock, DLockContext};
use std::{sync::Arc, time::Instant};

#[tokio::main]
async fn main() {
    // 开始计时
    let start_time = Instant::now();
    let ctx = DLockContext::new();
    let data_arc = Arc::new(DLock::new(5));

    let requests_count = 100;
    let mut handle_list = vec![];

    for _ in 0..requests_count {
        let handle_ctx = ctx.clone();
        let data = data_arc.clone();
        let lease_id = handle_ctx.apply_lease().unwrap();

        handle_list.push(tokio::spawn(async move {
            // println!("Lease ID: {:?} want get lock", lease_id);
            let mut value = data.lock(&handle_ctx, &lease_id).await.unwrap();
            // println!("Lease ID: {:?} get lock", lease_id);
            if *value > 0 {
                *value -= 1;
            }
            data.unlock();
            handle_ctx.cancel_lease(&lease_id);
            // println!("Lease ID: {:?} relieve lock", lease_id);
        }));
    }

    for handle in handle_list {
        handle.await.unwrap();
    }

    // 结束计时
    let end_time = Instant::now();

    // 计算程序运行时间（以纳秒为单位）
    let elapsed_time = end_time - start_time;

    // 打印程序运行时间（以秒为单位）
    println!("程序运行时间: {:.2?}", elapsed_time);
}
