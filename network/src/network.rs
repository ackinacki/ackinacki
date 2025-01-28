// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::time::Duration;

use crate::config::NetworkConfig;
use crate::detailed;
use crate::direct_sender;
use crate::gossip::GossipPeerResolver;
use crate::pub_sub::incoming_bridge;
use crate::pub_sub::outgoing_bridge;
use crate::pub_sub::Config;
use crate::NetworkMessage;
use crate::NetworkPeerId;

pub const BROADCAST_RETENTION_CAPACITY: usize = 100;
const DEFAULT_MAX_CONNECTIONS: usize = 200;

pub struct BasicNetwork<PeerId: NetworkPeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    config: NetworkConfig<PeerId>,
}

impl<PeerId: NetworkPeerId> BasicNetwork<PeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    pub fn from(config: NetworkConfig<PeerId>) -> Self {
        Self { config }
    }

    pub async fn start<Message: NetworkMessage + 'static>(
        &self,
        self_id: PeerId,
        incoming_sender: Sender<Message>,
        send_buffer_size: usize,
    ) -> anyhow::Result<(Sender<Message>, Sender<(PeerId, Message)>)> {
        tracing::info!("Starting network with configuration: {:?}", self.config);

        let (direct_sender, direct_receiver) = channel::<(PeerId, Message)>();

        let (config_sender, config_receiver) =
            tokio::sync::watch::channel::<Config>(self.config.pub_sub_config());

        let incoming_transport_sender = incoming_bridge(incoming_sender);
        let (outgoing_sender, outgoing_transport_sender) = outgoing_bridge();

        // listen for pub/sub connections
        tokio::spawn(async move {
            if let Err(e) = crate::pub_sub::run(
                DEFAULT_MAX_CONNECTIONS,
                config_receiver,
                outgoing_transport_sender,
                incoming_transport_sender,
            )
            .await
            {
                tracing::warn!("pub/sub failed: {}", detailed(&e));
            }
        });

        // listen config changes
        let config = self.config.clone();
        tokio::spawn(async move {
            if let Err(e) = run_config_refresher(config, config_sender).await {
                tracing::warn!("config store refresher failed: {}", detailed(&e));
            }
        });

        // listen for outgoing directed messages
        let config = self.config.pub_sub_config();
        let peer_resolver = GossipPeerResolver::new(self.config.gossip.clone());
        tokio::spawn(async move {
            if let Err(e) = direct_sender::run_direct_sender(
                self_id,
                direct_receiver,
                send_buffer_size,
                peer_resolver,
                config,
            )
            .await
            {
                tracing::warn!("direct sender failed: {}", detailed(&e));
            }
        });

        Ok((outgoing_sender, direct_sender))
    }
}

async fn run_config_refresher<PeerId: NetworkPeerId>(
    mut config: NetworkConfig<PeerId>,
    config_updates: tokio::sync::watch::Sender<Config>,
) -> anyhow::Result<()>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    let use_gossip = config.subscribe.is_empty();
    let mut addrs = config.nodes.values().copied().collect::<HashSet<_>>();
    loop {
        if use_gossip {
            config.refresh_alive_nodes(true).await;
            let new_addrs = config.nodes.values().copied().collect::<HashSet<_>>();
            tracing::info!("refresh network config: fetched addresses: {:?}", new_addrs);
            if new_addrs != addrs {
                addrs = new_addrs;
                tracing::trace!("pub/sub config store refresher: new addrs: {:?}", addrs);
                config_updates.send(config.pub_sub_config())?;
            }
        } else {
            tracing::warn!("pub/sub config refresher failed");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
