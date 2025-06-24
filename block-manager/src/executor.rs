// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::mpsc::channel;
use std::sync::Arc;

use message_router::message_router::MessageRouter;
use message_router::message_router::MessageRouterConfig;
use message_router::read_keys_from_file;
use network::try_parse_socket_addr;
use parking_lot::Mutex;
use salvo::conn::TcpListener;
use salvo::Listener;
use salvo::Server;

use crate::block_subscriber;
use crate::bp_resolver::BPResolverImpl;
use crate::cli::Args;
use crate::events;
use crate::license_root::get_license_addr;
use crate::rest_api_routes::rest_api_router;

pub async fn execute(args: Args) -> anyhow::Result<()> {
    // event bus
    let (event_pub, _event_sub) = channel::<events::Event>();
    // pass BP data (thread => IP addresses)
    let (bp_data_tx, bp_data_rx) = channel::<(String, Vec<String>)>();

    // message router
    let Ok(bind) = std::env::var("BLOCK_MANAGER_API") else {
        anyhow::bail!("BLOCK_MANAGER_API environment variable must be set");
    };

    let socket_addr = parse_socket_address(
        args.stream_src_url.host_str().expect("Host required"),
        args.stream_src_url.port_or_known_default().expect("Port required"),
    )?;

    let default_bp = try_parse_socket_addr(
        std::env::var("DEFAULT_BP").expect("DEFAULT_BP environment variable must be set"),
        crate::DEFAULT_BP_PORT,
    )
    .unwrap();

    let bp_resolver = Arc::new(Mutex::new(BPResolverImpl::new(default_bp)));
    BPResolverImpl::start_listener(Arc::clone(&bp_resolver), bp_data_rx)?;

    let Ok(license_number) = std::env::var("BM_LICENSE_NUMBER") else {
        anyhow::bail!("BM_LICENSE_NUMBER environment variable must be set");
    };

    let Ok(license_addr) = get_license_addr(default_bp, license_number).await else {
        anyhow::bail!("Failed to obtain license address");
    };

    let config = MessageRouterConfig {
        bp_resolver,
        license_addr,
        keys: std::env::var("BM_KEYS_FILE").ok().and_then(|path| read_keys_from_file(&path).ok()),
    };
    let _ = MessageRouter::new(bind, config);

    // REST API server
    let tcp_listener = TcpListener::new(args.rest_api);
    let acceptor = tcp_listener.try_bind().await?;

    let rest_api_server_handler = tokio::spawn(async move {
        Server::new(acceptor).serve(rest_api_router(default_bp)).await;
    });
    // block subscriber
    let block_subscriber = block_subscriber::BlockSubscriber::new(
        args.sqlite_path,
        socket_addr,
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
        _= rest_api_server_handler => {
             anyhow::bail!("rest_api_server_handler exited")
        },
        // _ = downloader_handler => {
        //     anyhow::bail!("downloader_handler exited")
        // }
        // _ = business_logic_handler => {
        //     anyhow::bail!("business_logic_handler exited")
        // }
    }
}

fn parse_socket_address(hostname: &str, port: u16) -> std::io::Result<SocketAddr> {
    let address = (hostname, port);
    // Try to resolve the hostname
    let mut addrs_iter = address.to_socket_addrs()?;
    addrs_iter.next().ok_or_else(|| std::io::Error::other("No address found for hostname"))
}
