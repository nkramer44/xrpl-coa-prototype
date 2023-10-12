// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::primary::{PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use crate::proposal::{SignedProposal};

const TIMER_RESOLUTION: u64 = 100;
const ACQUIRE_DELAY: u128 = 300;


fn clock() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}

pub struct ProposalWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,

    rx_network_proposal: Receiver<SignedProposal>,
    tx_loopback_proposal: Sender<SignedProposal>,
    to_acquire : VecDeque<(SignedProposal, u128)>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    batch_requests: HashSet<Digest>, // TODO cleanup or not
    pending: HashSet<Digest>,

    //consensus_round: Arc<AtomicU64>,    //TODO not to acquire for old proposals
}

impl ProposalWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        rx_network_proposal: Receiver<SignedProposal>,
        tx_loopback_proposal: Sender<SignedProposal>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                rx_network_proposal,
                tx_loopback_proposal,
                to_acquire : VecDeque::new(),
                network: SimpleSender::new(),
                batch_requests: HashSet::new(),
                pending: HashSet::new(),
            }
                .run()
                .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified Proposal.
    async fn batch_waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: SignedProposal,
        //mut handler: Receiver<()>,
    ) -> DagResult<Option<SignedProposal>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
           // _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(signed_proposal) = self.rx_network_proposal.recv() => {
                    //TODO verify sig

                    debug!("Synching the payload of {:?}", signed_proposal);
                    let batches = signed_proposal.proposal.batches.clone();

                    let mut missing = false;
                    for (digest, worker_id) in batches.iter() {
                        let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                        match self.store.read(key).await {
                            Ok(Some(_)) => {},
                            Ok(None) => {
                                missing = true;
                                break;
                            },
                            Err(e) => {
                                error!("{}", e);
                            }
                        }
                    }

                    if !missing {
                        self.tx_loopback_proposal
                        .send(signed_proposal)
                        .await //TODO need to wait?
                        .expect("Failed to send proposal");
                        continue;
                    }

                    let now = clock();
                    self.to_acquire.push_back((signed_proposal, now));
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(signed_proposal)) => {
                        let signed_proposal : SignedProposal = signed_proposal;
                        let pid = signed_proposal.proposal.clone().compute_id();//TODO clone
                        let _ = self.pending.remove(&pid);
                        for (x, _) in & signed_proposal.proposal.batches {
                            let _ = self.batch_requests.remove(x);
                        }
                        self.tx_loopback_proposal.send(signed_proposal).await.expect("Failed to send proposal");
                    },
                    Ok(None) => {
                        // This request has been canceled.
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    let now = clock();
                    loop{
                        let f = self.to_acquire.front();
                        if f.is_none() {
                            break;
                        }

                        let (_, t) = f.unwrap();
                        if (now - t) < ACQUIRE_DELAY {
                            break;
                        }

                        let (signed_proposal, _) = self.to_acquire.pop_front().unwrap();
                        let proposal_id = signed_proposal.proposal.clone().compute_id();
                        let author = signed_proposal.proposal.node_id.clone();
                        let batches = signed_proposal.proposal.batches.clone();

                        // Ensure we sync only once per proposal.
                        if self.pending.contains(&proposal_id) {
                            continue;
                        }

                        let mut missing = HashMap::new();
                        for (digest, worker_id) in batches.iter() {
                            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                            match self.store.read(key).await {
                                Ok(Some(_)) => {},
                                Ok(None) => {
                                    missing.insert(*digest, *worker_id);
                                },
                                Err(e) => {
                                    error!("{}", e);
                                },
                            }
                        }

                        if missing.is_empty() {
                            self.tx_loopback_proposal
                            .send(signed_proposal)
                            .await //TODO need to wait?
                            .expect("Failed to send proposal");
                            continue;
                        }

                        // Add the Proposal to the waiter pool. The waiter will return it to when all
                        // its parents are in the store.
                        let wait_for = missing
                            .iter()
                            .map(|(digest, worker_id)| {
                                let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                                (key.to_vec(), self.store.clone())
                            })
                            .collect();
                        //let (tx_cancel, rx_cancel) = channel(1);
                        self.pending.insert(proposal_id);//, (round, tx_cancel));
                        let fut = Self::batch_waiter(wait_for, signed_proposal);//, rx_cancel);
                        waiting.push(fut);

                        // Ensure we didn't already send a sync request for these batches.
                        let mut requires_sync = HashMap::new();
                        for (digest, worker_id) in missing.into_iter() {
                            if self.batch_requests.contains(&digest) {
                                continue;
                            }else{
                                self.batch_requests.insert(digest.clone());
                                requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                            }
                            // self.batch_requests.entry(digest.clone()).or_insert_with(|| {
                            //
                            //     round
                            // });
                        }
                        for (worker_id, digests) in requires_sync {
                            let address = self.committee
                            .worker(&author, &worker_id)
                            .expect("Author of valid Proposal is not in the committee")
                            .primary_to_worker;
                            let message = PrimaryWorkerMessage::Synchronize(digests, author);
                            let bytes = bincode::serialize(&message)
                            .expect("Failed to serialize batch sync request");
                            self.network.send(address, Bytes::from(bytes)).await;
                            //TODO understand the network topology
                        }
                    }

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}
// TODO don't acquire for old (previous consensus session) proposals
// TODO Cleanup internal state.
// TODO retry

// The commands that can be sent to the `Waiter`.
// #[derive(Debug)]
// pub enum ToWaiterMessage {
//     NetworkProposal(SignedProposal),
//     //SyncForValidation(Vec<Digest>, Validation),
// }
//
// #[derive(Debug)]
// pub enum FromWaiterMessage {
//     VerifiedProposal(SignedProposal),
//     //Validation(Validation),
// }
