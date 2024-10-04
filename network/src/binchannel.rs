// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

pub struct BinSender<T> {
    inner: Sender<T>,
}

impl<T> Clone for BinSender<T> {
    fn clone(&self) -> BinSender<T> {
        Self { inner: self.inner.clone() }
    }
}

unsafe impl<T: Send> Send for BinSender<T> {}
unsafe impl<T: Send> Sync for BinSender<T> {}

impl<T: Send> From<Sender<T>> for BinSender<T> {
    fn from(value: Sender<T>) -> Self {
        Self { inner: value }
    }
}

impl<T> BinSender<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    pub fn send(&self, value: &[u8]) -> anyhow::Result<()> {
        let t: T = bincode::deserialize::<T>(value).map_err(anyhow::Error::from)?;
        self.inner.send(t).map_err(|err| anyhow::anyhow!("send error: {}", err))
    }
}

pub struct BinReceiver<T> {
    inner: Receiver<T>,
}

impl<T> BinReceiver<T>
where
    T: serde::Serialize,
{
    pub fn from(inner: Receiver<T>) -> Self {
        Self { inner }
    }

    pub fn recv(&self) -> anyhow::Result<Vec<u8>> {
        self.inner
            .recv()
            .map_err(anyhow::Error::from)
            .and_then(|data| bincode::serialize(&data).map_err(anyhow::Error::from))
    }
}
