// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::channel;
use std::sync::Arc;

use message_router::message_router::MessageRouter;
use parking_lot::Mutex;

use crate::block_subscriber;
use crate::bp_resolver::BPResolverImpl;
use crate::cli::Args;
use crate::events;
// use crate::state;

pub async fn execute(args: Args) -> anyhow::Result<()> {
    // event bus
    let (event_pub, _event_sub) = channel::<events::Event>();
    // pass BP data (thread => IP addresses)
    let (bp_data_tx, bp_data_rx) = channel::<(String, Vec<String>)>();

    // message router
    let Ok(bind_to) = std::env::var("BLOCK_MANAGER_API") else {
        anyhow::bail!("BLOCK_MANAGER_API environment variable must be set");
    };

    let bp_resolver = Arc::new(Mutex::new(BPResolverImpl::new()));
    BPResolverImpl::start_listener(Arc::clone(&bp_resolver), bp_data_rx)?;
    let _ = MessageRouter::new(bind_to, bp_resolver);

    // block subscriber
    let block_subscriber = block_subscriber::BlockSubscriber::new(
        args.sqlite_path,
        args.stream_src_url,
        event_pub.clone(),
        bp_data_tx,
    );
    let block_subscriber_handler = block_subscriber.run();

    // // state downloader
    // let (state_downloader_pub, state_downloader_sub) = channel();
    // let downloader = state::downloader::StateDownloader::new(
    //     args.http_src_url,
    //     state_downloader_sub,
    //     event_pub.clone(),
    // );
    // let downloader_handler = {
    //     let handler = downloader.run();
    //     // initial download
    //     state_downloader_pub.send(()).expect("send initial message to downloader");
    //     handler
    // };

    // // business logic
    // let business_logic_handler = tokio::spawn(async move {
    //     // business logic
    //     loop {
    //         let _ = event_sub.recv().unwrap();
    //     }
    //     // todo!();
    // });
    // // download state

    tokio::select! {
        _ = block_subscriber_handler => {
            anyhow::bail!("block_subscriber_handler exited")
        },
        // _ = downloader_handler => {
        //     anyhow::bail!("downloader_handler exited")
        // }
        // _ = business_logic_handler => {
        //     anyhow::bail!("business_logic_handler exited")
        // }
    }
}
