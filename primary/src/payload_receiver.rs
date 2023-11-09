// Copyright(C) Facebook, Inc. and its affiliates.
use config::WorkerId;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::{Sender, Receiver};
use crate::Batches;
use log::{debug, info};

/// Receives batches' digests of other authorities. These are only needed to verify incoming
/// headers (ie. make sure we have their payload).
pub struct PayloadReceiver {
    /// The persistent storage.
    store: Store,
    /// Receives batches' digests from the network.

    rx_store: Receiver<(Digest, WorkerId)>,
    tx_proposal_waiter: Sender<Batches>,
    tx_consensus: Sender<Batches>,
    batch_buf: Vec<(Digest, WorkerId)>,
}

impl PayloadReceiver {
    pub fn spawn(store: Store,
                 rx_store: Receiver<(Digest, WorkerId)>,
                 tx_proposal_waiter: Sender<Batches>,
                 tx_consensus: Sender<Batches>) {
        tokio::spawn(async move {
            Self { store, rx_store, tx_proposal_waiter, tx_consensus,
                batch_buf: Vec::with_capacity(100) }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some((batch, worker_id)) = self.rx_store.recv().await {
            if *batch.0.get(0).unwrap() == 0 as u8 {
                #[cfg(feature = "benchmark")]
                info!("Created {:?}", batch);
            }
            self.batch_buf.push((batch, worker_id));
            if self.batch_buf.len() >= 100 {
                //debug!("sending batches");
                let batches = Batches::Batches(self.batch_buf.clone().drain(..).collect());
                self.tx_consensus.send(batches).await;
                let batches = Batches::Batches(self.batch_buf.drain(..).collect());
                self.tx_proposal_waiter.send(batches).await;
            }
        }
    }
}
