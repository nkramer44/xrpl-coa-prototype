// Copyright(C) Facebook, Inc. and its affiliates.
//use crate::error::{DagError, DagResult};

use crate::primary::{LedgerOrValidation, Batches, PrimaryPrimaryMessage};
use bytes::Bytes;
use config::Committee;
use crypto::{PublicKey};
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use crate::{ConsensusPrimaryMessage, Ledger, PrimaryConsensusMessage, SignedValidation};
use crate::proposal::{SignedProposal};

// #[cfg(test)]
// #[path = "tests/core_tests.rs"]
// pub mod core_tests;

const TIMER_RESOLUTION: u64 = 1000;

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,

    tx_primary_consensus: Sender<PrimaryConsensusMessage>,
    rx_consensus_primary: Receiver<ConsensusPrimaryMessage>,
    rx_loopback_proposal: Receiver<SignedProposal>,
    rx_loopback_validations_ledgers: Receiver<LedgerOrValidation>,
    tx_own_ledgers: Sender<LedgerOrValidation>,

    /// A network sender to send the batches to the other workers.
    network: ReliableSender,

    /// TODO: We need some way to clean this Vec up.
    cancel_handlers: Vec<CancelHandler>,
    timeout_count: u32,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        tx_primary_consensus: Sender<PrimaryConsensusMessage>,
        rx_consensus_primary: Receiver<ConsensusPrimaryMessage>,
        rx_loopback_proposal: Receiver<SignedProposal>,
        rx_loopback_validations_ledgers: Receiver<LedgerOrValidation>,
        tx_own_ledgers: Sender<LedgerOrValidation>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                tx_primary_consensus,
                rx_consensus_primary,
                rx_loopback_proposal,
                rx_loopback_validations_ledgers,
                tx_own_ledgers,
                network: ReliableSender::new(),
                cancel_handlers: vec![],
                timeout_count: 0,
            }
                .run()
                .await;
        });
    }

    async fn process_timer_event(&mut self) {
        debug!("Sending timer {}", self.timeout_count);
        self.tx_primary_consensus
            .send(PrimaryConsensusMessage::Timeout(self.timeout_count))
            .await
            .expect("Failed to send timeout");
        self.timeout_count += 1;
    }

    async fn process_proposal(&mut self, proposal: SignedProposal) {
        //-> DagResult<()> {
        //debug!("Sending proposal to consensus {:?}", proposal);
        self.tx_primary_consensus
            .send(PrimaryConsensusMessage::Proposal(proposal))
            .await
            .expect("Failed to send proposal");
    }

    async fn process_validation(&mut self, validation: SignedValidation) {
        //-> DagResult<()> {
        // debug!("Processing {:?}", validation);
        self.tx_primary_consensus
            .send(PrimaryConsensusMessage::Validation(validation))
            .await
            .expect("Failed to send validation");
    }

    async fn process_ledger(&mut self, ledger: Ledger) {
        //-> DagResult<()> {
        // debug!("Processing {:?}", ledger);
        self.tx_primary_consensus
            .send(PrimaryConsensusMessage::SyncedLedger(ledger))
            .await
            .expect("Failed to send ledger");
    }

    async fn process_own_proposal(&mut self, proposal: SignedProposal) {
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryPrimaryMessage::Proposal(proposal.clone()))
            .expect("Failed to serialize our own proposal");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers.extend(handlers);
    }

    async fn process_own_validation(&mut self, validation: SignedValidation) {
        self.tx_own_ledgers.send(LedgerOrValidation::Validation(validation)).await.expect("cannot send self ledger to waiter");
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryPrimaryMessage::Validation(validation))
            .expect("Failed to serialize our own validation");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers.extend(handlers);
    }

    async fn process_own_ledger(&mut self, ledger: Ledger) {
        self.tx_own_ledgers.send(LedgerOrValidation::Ledger(ledger)).await.expect("cannot send self ledger to waiter");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(signed_proposal) = self.rx_loopback_proposal.recv() =>
                self.process_proposal(signed_proposal).await,

                Some(ledger_or_validation) = self.rx_loopback_validations_ledgers.recv() => {
                    match ledger_or_validation {
                        LedgerOrValidation::Ledger(ledger) => self.process_ledger(ledger).await,
                        LedgerOrValidation::Validation(validation) => self.process_validation(validation).await
                    }
                },

                Some(cp_message) = self.rx_consensus_primary.recv() => {
                    match cp_message {
                        ConsensusPrimaryMessage::Proposal(proposal) => self.process_own_proposal(proposal.as_ref().clone()).await,
                        ConsensusPrimaryMessage::Validation(validation) => self.process_own_validation(validation).await,
                        ConsensusPrimaryMessage::NewLedger(ledger) => self.process_own_ledger(ledger).await,
                    }
                }

                () = &mut timer => {
                    self.process_timer_event().await;
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}
