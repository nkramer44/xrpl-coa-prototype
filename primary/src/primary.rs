// Copyright(C) Facebook, Inc. and its affiliates.
use crate::validation_waiter::{ValidationWaiter};
use crate::core::Core;
use crate::error::DagError;
use crate::proposal_waiter::{ProposalWaiter};
use crate::helper::Helper;
use crate::payload_receiver::PayloadReceiver;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::{Ledger, SignedValidation};
use crate::proposal::SignedProposal;

/// The default channel capacity for each channel of the primary.
pub const CHANNEL_CAPACITY: usize = 1_000_000;

/// The round number.
pub type Round = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryPrimaryMessage {
    Proposal(SignedProposal),
    Validation(SignedValidation),
    LedgerRequest(Vec<Digest>, /* requestor */ PublicKey), //TODO PublicKey
    Ledger(Ledger),
}

/// The messages sent by the primary to its workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum PrimaryWorkerMessage {
    /// The primary indicates that the worker need to sync the target missing batches.
    Synchronize(Vec<Digest>, /* target */ PublicKey),
    /// The primary indicates a round update.
    Cleanup(Round), //TODO
}

/// The messages sent by the workers to their primary.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerPrimaryMessage {
    /// The worker indicates it sealed a new batch.
    OurBatch(Digest, WorkerId),
    /// The worker indicates it received a batch's digest from another authority.
    OthersBatch(Digest, WorkerId),
}

#[derive(Debug)]
pub enum PrimaryConsensusMessage {
    Timeout(u32),
    Proposal(SignedProposal),
    SyncedLedger(Ledger),
    Validation(SignedValidation),
}

#[derive(Debug)]
pub enum Batches {
    Batches(Vec<(Digest, WorkerId)>)
}

#[derive(Debug)]
pub enum ConsensusPrimaryMessage {
    Proposal(Arc<SignedProposal>),
    Validation(SignedValidation),
    NewLedger(Ledger),
}

#[derive(Debug)]
pub enum LedgerOrValidation {
    Ledger(Ledger),
    Validation(SignedValidation)
}

pub struct Primary;

impl Primary {
    pub fn spawn(
        public_key: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_primary_consensus: Sender<PrimaryConsensusMessage>,
        tx_primary_consensus_data: Sender<Batches>,
        rx_consensus_primary: Receiver<ConsensusPrimaryMessage>,
    ) {
        let (tx_worker_batches, rx_worker_batches) = channel(CHANNEL_CAPACITY);
        let (tx_proposal_waiter_batches, rx_proposal_waiter_batches) = channel(CHANNEL_CAPACITY);

        let (tx_network_proposals, rx_network_proposals) = channel(CHANNEL_CAPACITY);
        let (tx_network_validations, rx_network_validations) = channel(CHANNEL_CAPACITY);
        let (tx_network_ledgers, rx_network_ledgers) = channel(CHANNEL_CAPACITY);
        let (tx_ledger_requests, rx_ledger_requests) = channel(CHANNEL_CAPACITY);

        let (tx_loopback_proposals, rx_loopback_proposals) = channel(CHANNEL_CAPACITY);
        let (tx_loopback_validations_ledgers, rx_loopback_validations_ledgers) = channel(CHANNEL_CAPACITY);

        let (tx_full_validated_ledgers, rx_full_validated_ledgers) = channel(CHANNEL_CAPACITY);
        let (tx_own_ledgers, rx_own_ledgers) = channel(CHANNEL_CAPACITY);

        parameters.log();

        let name = public_key;

        // Spawn the network receiver listening to messages from the other primaries.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            PrimaryReceiverHandler {
                tx_network_proposals,
                tx_network_validations,
                tx_network_ledgers,
                tx_ledger_requests,
            },
        );
        info!(
            "Primary {} listening to primary messages on {}",
            name, address
        );

        // Spawn the network receiver listening to messages from our workers.
        let mut address = committee
            .primary(&name)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_primary;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            WorkerReceiverHandler {
                tx_worker_batches,
                //tx_store_batches,
            },
        );
        info!(
            "Primary {} listening to workers messages on {}",
            name, address
        );

        // Receives batch digests from workers. They are only used to validate proposals.
        PayloadReceiver::spawn(store.clone(), rx_worker_batches, tx_proposal_waiter_batches, tx_primary_consensus_data);

        // The `Helper` is dedicated to reply to ledger requests from other primaries.
        Helper::spawn(committee.clone(), store.clone(), rx_ledger_requests);

        ProposalWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            rx_proposal_waiter_batches,
            rx_network_proposals,
            tx_loopback_proposals,
            rx_full_validated_ledgers,
        );

        ValidationWaiter::spawn(
            name,
            committee.clone(),
            store.clone(),
            rx_network_validations,
            rx_network_ledgers,
            tx_loopback_validations_ledgers,
            rx_own_ledgers,
            tx_full_validated_ledgers,
        );

        Core::spawn(
            name,
            committee.clone(),
            store.clone(),
            tx_primary_consensus,
            rx_consensus_primary,
            rx_loopback_proposals,
            rx_loopback_validations_ledgers,
            tx_own_ledgers,
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            name,
            committee
                .primary(&name)
                .expect("Our public key or worker id is not in the committee")
                .primary_to_primary
                .ip()
        );
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_network_proposals: Sender<SignedProposal>,
    tx_network_validations: Sender<SignedValidation>,
    tx_network_ledgers: Sender<Ledger>,
    tx_ledger_requests: Sender<(Vec<Digest>, PublicKey)>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            PrimaryPrimaryMessage::Proposal(signed_proposal) => self
                .tx_network_proposals
                .send(signed_proposal)
                .await
                .expect("Failed to send proposal"),
            PrimaryPrimaryMessage::Validation(signed_validation) => self
                .tx_network_validations
                .send(signed_validation)
                .await
                .expect("Failed to send validation"),
            PrimaryPrimaryMessage::Ledger(ledger) => self
                .tx_network_ledgers
                .send(ledger)
                .await
                .expect("Failed to send ledger"),
            PrimaryPrimaryMessage::LedgerRequest(missing, requestor) => self
                .tx_ledger_requests
                .send((missing, requestor))
                .await
                .expect("Failed to send ledger request"),
        }

        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_worker_batches: Sender<(Digest, WorkerId)>,
//    tx_store_batches: Sender<(Digest, WorkerId)>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize(&serialized).map_err(DagError::SerializationError)? {
            WorkerPrimaryMessage::OurBatch(digest, worker_id) => {
                self
                    .tx_worker_batches
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests");
            }
            WorkerPrimaryMessage::OthersBatch(digest, worker_id) => {
                self
                    .tx_worker_batches
                    .send((digest, worker_id))
                    .await
                    .expect("Failed to send workers' digests");
            }
        }

        Ok(())
    }
}
