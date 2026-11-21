//! Private transport for a journal and its clients running in one process.

use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::sync::LazyLock;

use parking_lot::Mutex;
use tokio::io::{DuplexStream, duplex};
use tokio::sync::mpsc;

const PREFIX: &str = "local://";
const BUFFER_BYTES: usize = 64 * 1024;
const WAITING_CONNECTIONS: usize = 16;

static LISTENERS: LazyLock<Mutex<HashMap<String, mpsc::Sender<DuplexStream>>>> =
    LazyLock::new(Default::default);

/// The exclusive registration of one in-process journal address.
pub struct LocalListener {
    address: String,
    incoming: mpsc::Receiver<DuplexStream>,
}

impl LocalListener {
    pub fn bind(address: &str) -> io::Result<Self> {
        if !is_local(address) || address.len() == PREFIX.len() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid local journal address",
            ));
        }

        let (sender, incoming) = mpsc::channel(WAITING_CONNECTIONS);
        let mut listeners = LISTENERS.lock();
        if listeners.contains_key(address) {
            return Err(io::Error::new(
                ErrorKind::AddrInUse,
                "local journal address is occupied",
            ));
        }

        listeners.insert(address.to_owned(), sender);
        Ok(Self {
            address: address.to_owned(),
            incoming,
        })
    }

    pub(crate) async fn accept(&mut self) -> io::Result<DuplexStream> {
        self.incoming
            .recv()
            .await
            .ok_or_else(|| io::Error::new(ErrorKind::BrokenPipe, "local journal listener closed"))
    }
}

impl Drop for LocalListener {
    fn drop(&mut self) {
        LISTENERS.lock().remove(&self.address);
    }
}

pub(crate) fn is_local(address: &str) -> bool {
    address.starts_with(PREFIX)
}

pub(crate) async fn connect(address: &str) -> io::Result<DuplexStream> {
    let unavailable =
        || io::Error::new(ErrorKind::ConnectionRefused, "local journal is unavailable");
    let listener = LISTENERS
        .lock()
        .get(address)
        .cloned()
        .ok_or_else(unavailable)?;
    let (client, server) = duplex(BUFFER_BYTES);
    listener.send(server).await.map_err(|_| unavailable())?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test]
    async fn a_journal_address_is_private_to_its_registered_listener() {
        let address = "local://a-journal";
        assert!(connect(address).await.is_err());
        let mut listener = LocalListener::bind(address).unwrap();
        assert!(LocalListener::bind(address).is_err());
        let mut client = connect(address).await.unwrap();
        let mut server = listener.accept().await.unwrap();
        client.write_all(b"ordered").await.unwrap();
        let mut message = [0u8; 7];
        server.read_exact(&mut message).await.unwrap();
        assert_eq!(&message, b"ordered");
        drop(listener);
        assert!(connect(address).await.is_err());
    }
}
