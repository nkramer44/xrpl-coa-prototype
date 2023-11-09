// Copyright(C) Facebook, Inc. and its affiliates.

use crate::primary::{PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};

use futures::stream::StreamExt as _;
use log::{debug, error, info};
use network::{SimpleSender};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use crate::{Batches, Ledger};
use crate::proposal::{SignedProposal};

const TIMER_RESOLUTION: u64 = 100;
const ACQUIRE_DELAY: u128 = 300;


fn clock() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}

struct Dependencies {
    missing_counts : HashMap<Digest, usize>,
    dependencies : HashMap<(Digest, WorkerId), Vec<Digest>>,
}

impl Dependencies {
    pub fn new() -> Self {
        Self {
            missing_counts : HashMap::new(),
            dependencies: HashMap::new(),
        }
    }

    pub fn addDependencies(&mut self, missing : &Vec<(Digest, WorkerId)>, pid:&Digest){
        info!("D-CHECK adding proposal {:?}, missing {}", pid, missing.len());
        if ! self.missing_counts.contains_key(pid) {
            self.missing_counts.insert(pid.clone(), missing.len());
            for m in missing {
                self.dependencies.entry(m.clone()).or_insert_with(Vec::new).push(pid.clone());
            }
        }
    }

    pub fn addBatches(&mut self, batches: &Vec<(Digest, WorkerId)>) -> Vec<Digest>{
        let mut pids = Vec::new();
        for b in batches {
            match self.dependencies.remove(b) {
                None => {}
                Some(ps) => {
                    for p in ps {
                        match self.missing_counts.get_mut(&p){
                            None => {
                                error!("Dependencies error");
                            }
                            Some(c) => {
                                *c = *c- 1;
                                //debug!("D-CHECK {} has {} missing", p, *c);
                                if *c == 0 {
                                    self.missing_counts.remove(&p);
                                    info!("D-CHECK Synced proposal {:?} should be delivered", p);
                                    pids.push(p);
                                }
                            }
                        }
                    }
                }
            }
        }
        pids
    }
}

pub struct ProposalWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,

    rx_batches: Receiver<Batches>,
    rx_network_proposal: Receiver<SignedProposal>,
    tx_loopback_proposal: Sender<SignedProposal>,
    rx_ledgers: Receiver<Vec<Ledger>>,

    batch_cache: HashSet<(Digest, WorkerId)>,
    to_acquire: VecDeque<(u128, Vec<(Digest, WorkerId)>, PublicKey)>,

    /// Network driver allowing to send messages.
    network: SimpleSender,
    batch_requests: HashSet<Digest>, // TODO cleanup or not
    pending: HashMap<Digest, SignedProposal>,
    dependencies: Dependencies,
}

impl ProposalWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        rx_batches: Receiver<Batches>,
        rx_network_proposal: Receiver<SignedProposal>,
        tx_loopback_proposal: Sender<SignedProposal>,
        rx_ledgers: Receiver<Vec<Ledger>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                //store,
                rx_batches,
                rx_network_proposal,
                tx_loopback_proposal,
                rx_ledgers,//rx_from_core,
                batch_cache: HashSet::new(),
                to_acquire: VecDeque::new(),
                network: SimpleSender::new(),
                batch_requests: HashSet::new(),
                pending: HashMap::new(),
                dependencies: Dependencies::new(),
            }
                .run()
                .await;
        });
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        //let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_batches.recv() => {
                    match message {
                        Batches::Batches(batches) => {
                            for pid in self.dependencies.addBatches(&batches){
                                match self.pending.remove(&pid) {
                                    Some(proposal) => {
                                        debug!("(2) Send proposal {:?}", proposal);
                                        self.tx_loopback_proposal.send(proposal)
                                        .await.expect("Failed to send proposal");
                                    },
                                    None => {
                                        panic!("Cannot find synced proposal");
                                    },
                                }
                            }

                            for (batch, wid) in batches{
                                self.batch_cache.insert((batch, wid));
                            }
                        }
                    }
                },

                Some(signed_proposal) = self.rx_network_proposal.recv() => {
                    let proposal_id = signed_proposal.proposal.compute_id();
                    // Ensure we sync only once per proposal.
                    if self.pending.contains_key(&proposal_id) {
                        continue;
                    }

                    let batches = &signed_proposal.proposal.batches;
                    let mut missing = Vec::new();
                    for batch in batches {
                        if ! self.batch_cache.contains(batch) {
                            missing.push(batch.clone());
                        }
                    }

                    // info!("Proposal {:?} missing {:?}", signed_proposal, missing.keys().collect::<Vec<&Digest>>());
                    if missing.is_empty() {
                        debug!("(1) Send proposal {:?}", signed_proposal);
                        self.tx_loopback_proposal
                        .send(signed_proposal)
                        .await
                        .expect("Failed to send proposal");
                        continue;
                    }

                    debug!("Waiting proposal {:?}, missing {}", signed_proposal, missing.len());
                    let now = clock();
                    let author = signed_proposal.proposal.node_id.clone();
                    self.dependencies.addDependencies(&missing, &proposal_id);
                    self.to_acquire.push_back((now, missing, author));
                    self.pending.insert(proposal_id, signed_proposal);
                },

                Some(ledgers) = self.rx_ledgers.recv() => {
                    for l in ledgers{
                        info!("fully validated ledger {:?} {}", l.id, l.seq);
                        for batch in l.batch_set{
                            self.batch_cache.remove(&batch);
                        }
                    }
                },

                () = &mut timer => {
                    //debug!("TIMEOUT");
                    let now = clock();
                    loop{
                        let f = self.to_acquire.front();
                        if f.is_none() {
                            break;
                        }

                        let (t, _, _) = f.unwrap();
                        if (now - t) < ACQUIRE_DELAY {
                            break;
                        }

                        let (_, missing, author) = self.to_acquire.pop_front().unwrap();

                        let mut still_missing = Vec::new();
                        for batch in missing {
                            if ! self.batch_cache.contains(&batch) {
                                still_missing.push(batch);
                            }
                        }

                        /*info!(
                            "Proposal {:?} missing {:?}",
                            (author, signed_proposal.proposal.parent_id, signed_proposal.proposal.round),
                            missing.keys().collect::<Vec<&Digest>>()
                        );*/
                        if still_missing.is_empty() {
                            continue;
                        }

                        // Ensure we didn't already send a sync request for these batches.
                        let mut requires_sync = HashMap::new();
                        for (digest, worker_id) in still_missing.into_iter() {
                            if self.batch_requests.contains(&digest) {
                                continue;
                            }else{
                                self.batch_requests.insert(digest.clone());
                                requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                            }
                        }
                        for (worker_id, digests) in requires_sync {
                            let address = self.committee
                            .worker(&self.name, &worker_id)
                            .expect("Author of valid Proposal is not in the committee")
                            .primary_to_worker;
                            let message = PrimaryWorkerMessage::Synchronize(digests, author);
                            let bytes = bincode::serialize(&message)
                            .expect("Failed to serialize batch sync request");
                            self.network.send(address, Bytes::from(bytes)).await;
                        }
                    }
                    //debug!("RESCHEDULE");
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}
// TODO don't acquire for old (previous consensus session) proposals
