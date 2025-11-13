// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

use url::Url;

use crate::events::Event;

pub struct StateDownloader {
    url: Url,
    rx: Receiver<()>,
    event_pub: Sender<Event>,
}

impl StateDownloader {
    pub fn new(url: Url, rx: Receiver<()>, event_pub: Sender<Event>) -> Self {
        Self { url, rx, event_pub }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        loop {
            match self.rx.recv() {
                Ok(()) => {
                    // TODO: finish implementation + retry download + continue download mechanism
                    let _ = reqwest::get(self.url.clone()).await?;
                    self.event_pub.send(Event::StateDownloaded).expect("Failed to send event");
                }
                Err(err) => {
                    anyhow::bail!("Fail to receive channel message {err}");
                }
            }
        }
    }
}
