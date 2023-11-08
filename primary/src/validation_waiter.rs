// Copyright(C) Facebook, Inc. and its affiliates.
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use async_recursion::async_recursion;
use bytes::Bytes;
use log::{debug, error, info};
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use xrpl_consensus_core::{LedgerIndex, Validation};
use config::Committee;
use crypto::{Digest, PublicKey};
use network::SimpleSender;
use crate::{Ledger, SignedValidation};
use crate::primary::{PrimaryPrimaryMessage, LedgerOrValidation};

const TIMER_RESOLUTION: u64 = 100;
const ACQUIRE_DELAY: u128 = 300;

fn clock() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}

struct LedgerMaster {
    //tree : HashMap<LedgerIndex, HashMap<Digest, (Option<Ledger>, Vec<SignedValidation>)>>,
    tx_full_validated_ledgers: Sender<Vec<Ledger>>,
    index_to_hash: HashMap<LedgerIndex, HashSet<Digest>>,
    hash_to_ledger: HashMap<Digest, Ledger>,
    hash_to_validators: HashMap<Digest, HashSet<PublicKey>>,
    pub fully_validated: LedgerIndex,
    pub quorum: usize,
}

impl LedgerMaster {
    pub fn new(tx_full_validated_ledgers: Sender<Vec<Ledger>>,
               quorum: usize) -> Self {
        Self {
            tx_full_validated_ledgers,
            index_to_hash: HashMap::new(),
            hash_to_ledger: HashMap::new(),
            hash_to_validators: HashMap::new(),
            fully_validated: 0,
            quorum,
        }
    }

    async fn check_fully_validated(&mut self, lsqn: LedgerIndex, lid: Digest) {
        let full = match self.hash_to_validators.get(&lid) {
            Some(validators) => { validators.len() >= self.quorum },
            None => false,
        };

        info!("checking fully validated, ledger {:?} {}", lid, full);

        if full {
            let mut lsqn = lsqn;
            let mut lid = &lid;
            let mut hashes = Vec::new();
            let need = (lsqn - self.fully_validated) as usize;
            while lsqn > self.fully_validated {
                match self.hash_to_ledger.get(&lid) {
                    None => {
                        break;
                    }
                    Some(l) => {
                        hashes.push(lid.clone());
                        lsqn -= 1;
                        lid = &l.ancestors.last().unwrap();
                    }
                }
            }

            info!("checking fully validated, need {} got {}", need, hashes.len());

            if hashes.len() == need {
                self.fully_validated += need as u32;
                let mut ledgers: Vec<Ledger> = Vec::new();
                while !hashes.is_empty() {
                    let lid = hashes.pop().unwrap();
                    ledgers.push(self.hash_to_ledger.remove(&lid).unwrap());
                }

                #[cfg(feature = "benchmark")]
                for l in &ledgers {
                    for (batch, _) in &l.batch_set {
                        if *batch.0.get(0).unwrap() == 0 as u8 {
                            info!("Committed {:?} ", batch);
                        }
                    }
                }
                self.tx_full_validated_ledgers.send(ledgers).await;
            }
        }
    }

    pub async fn add_ledger(&mut self, l: Ledger) {
        let lsqn = l.seq;
        info!("add_ledger {} {}", lsqn, self.fully_validated);
        if lsqn > self.fully_validated {
            let lid = l.id.clone();
            if !self.hash_to_ledger.contains_key(&lid) {
                self.hash_to_ledger.insert(lid.clone(), l);
            }
            self.check_fully_validated(lsqn, lid).await;
        }
    }

    pub async fn add_validation(&mut self, v: &SignedValidation) {
        let lsqn = v.seq();
        info!("add_validation {} {}", lsqn, self.fully_validated);
        if lsqn > self.fully_validated {
            let lid = v.ledger_id();
            self.hash_to_validators.entry(lid)
                .or_insert_with(HashSet::new)
                .insert(v.validation.node_id);
            self.check_fully_validated(lsqn, lid).await;
        }
    }
}

/// Waits to receive all the ancestors of a Validation before looping it back to the `Core`
/// for further processing.
pub struct ValidationWaiter {
    ledger_master: LedgerMaster,
    name: PublicKey,
    committee: Committee,
    store: Store,
    rx_network_validations: Receiver<SignedValidation>,
    rx_network_ledgers: Receiver<Ledger>,
    tx_loopback_validations_ledgers: Sender<LedgerOrValidation>,
    rx_own_ledgers: Receiver<LedgerOrValidation>,

    to_acquire: VecDeque<(SignedValidation, u128)>,
    validation_dependencies: HashMap<Digest, Vec<SignedValidation>>,
    ledger_dependencies: HashMap<Digest, (Vec<Ledger>, PublicKey)>, // contains pending acquires

    /// Network driver allowing to send messages.
    network: SimpleSender,
}

impl ValidationWaiter {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        rx_network_validations: Receiver<SignedValidation>,
        rx_network_ledgers: Receiver<Ledger>,
        tx_loopback_validations_ledgers: Sender<LedgerOrValidation>,
        rx_own_ledgers: Receiver<LedgerOrValidation>,

        tx_full_validated_ledgers: Sender<Vec<Ledger>>,
    ) {
        tokio::spawn(async move {
            Self {
                ledger_master: LedgerMaster::new(
                    tx_full_validated_ledgers,
                    (committee.authorities.len() as f32 * 0.80).ceil() as usize, ),
                name,
                committee,
                store,
                rx_network_validations,
                rx_network_ledgers,
                tx_loopback_validations_ledgers,
                rx_own_ledgers,
                to_acquire: VecDeque::new(),
                validation_dependencies: HashMap::new(),
                ledger_dependencies: HashMap::new(),
                network: SimpleSender::new(),
            }
                .run()
                .await
        });
    }

    async fn try_deliver(&mut self, ledger_id: &Digest) {
        match self.validation_dependencies.remove(ledger_id) {
            Some(signed_validations) => {
                for signed_validation in signed_validations.into_iter() {
                    self.tx_loopback_validations_ledgers.send(LedgerOrValidation::Validation(signed_validation))
                        .await
                        .expect("TODO: panic message");
                }
            },
            None => {}
        }

        let mut to_deliver = VecDeque::new();
        self.to_acquire.retain(| (v, _) | return if v.ledger_id() == *ledger_id {
            to_deliver.push_back(v.clone());
            false
        } else {
            true
        });
        for v in to_deliver {
            self.tx_loopback_validations_ledgers.send(LedgerOrValidation::Validation(v))
                .await
                .expect("TODO: panic message");
        }
    }

    #[async_recursion]
    async fn store_children(&mut self, parent_id: &Digest) {
        match self.ledger_dependencies.remove(parent_id) {
            Some((children, _)) => {
                for ledger in children.into_iter() {
                    self.store_ledger(ledger, false).await;
                }
            }
            None => {}
        }
    }

    async fn store_ledger(&mut self, ledger: Ledger, own: bool){
        self.store.write(ledger.id.to_vec(), bincode::serialize(&ledger).unwrap()).await;
        let lid = ledger.id.clone();
        if !own {
            self.tx_loopback_validations_ledgers.send(LedgerOrValidation::Ledger(ledger))
                .await
                .expect("TODO: panic message");
        }
        self.try_deliver(&lid).await;
        self.store_children(&lid).await;
    }

    async fn try_store_ledger(&mut self, ledger: Ledger) -> Option<(Digest, PublicKey)> {
        if ledger.ancestors.is_empty() || self.ledger_dependencies.get(&ledger.id).is_none() {
            return None;
        }

        let parent = ledger.ancestors.last().unwrap().clone();
        match self.store.read(parent.to_vec()).await {
            Ok(Some(_)) => {
                self.store_ledger(ledger, false).await;
                return None;
            }
            Ok(None) => {
                let (_, pk) = self.ledger_dependencies.get(&ledger.id).unwrap();
                let pk = pk.clone();
                // self.ledger_dependencies.entry(parent).or_insert_with(Vec::new).push(ledger);

                if let Some((ledgers, _)) = self.ledger_dependencies.get_mut(&parent)
                {
                    ledgers.push(ledger);
                }else{
                    let mut ledgers = Vec::new();
                    ledgers.push(ledger);
                    self.ledger_dependencies.insert(parent.clone(), (ledgers, pk.clone()));
                }
                return Some((parent, pk));
            }
            Err(e) => {
                error!("Failed to store ledger. {}", e);
                return None;
            }
        }
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);
        info!("quorum {}", self.ledger_master.quorum);

        loop {
            tokio::select! {
                Some(signed_validation) = self.rx_network_validations.recv() => {
                    //TODO verify sig
                    info!("Network validation {:?} {}, fully validated {}",
                        signed_validation.validation.ledger_id,
                        signed_validation.validation.seq,
                        self.ledger_master.fully_validated);
                    self.ledger_master.add_validation(&signed_validation).await;

                    let ledger_id = signed_validation.validation.ledger_id;
                    match self.store.read(ledger_id.to_vec()).await{
                        Ok(Some(_)) => {
                            self.tx_loopback_validations_ledgers
                            .send(LedgerOrValidation::Validation(signed_validation))
                            .await
                            .expect("Failed to send validation");
                        }
                        Ok(None) => {
                            info!("Need to acquire ledger {:?}", signed_validation.validation.ledger_id);
                            self.to_acquire.push_back((signed_validation, clock()));
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                },

                Some(ledger) = self.rx_network_ledgers.recv() => {
                    //TODO verify ledger
                    info!("Network ledger {:?} {}, fully validated {}",
                        ledger.id, ledger.seq, self.ledger_master.fully_validated);
                    self.ledger_master.add_ledger(ledger.clone()).await;

                    match self.try_store_ledger(ledger).await {
                        Some((digest, pk)) => {
                            // acquire(digest, pk).await;
                            let address = self.committee
                                .primary(&pk)
                                .expect("Author is not in the committee")
                                .primary_to_primary;
                            let mut digests = vec![];
                            digests.push(digest);
                            let message = PrimaryPrimaryMessage::LedgerRequest(digests, self.name);
                            let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize batch sync request");
                            self.network.send(address, Bytes::from(bytes)).await;
                        },
                        None => {}
                    }
                },

                Some(ledger_or_validation) = self.rx_own_ledgers.recv() => {
                    match ledger_or_validation {
                        LedgerOrValidation::Ledger(ledger) => {
                            info!("Own ledger {:?} {}, fully validated {}",
                                ledger.id, ledger.seq, self.ledger_master.fully_validated);
                            self.ledger_master.add_ledger(ledger.clone()).await;
                            self.store_ledger(ledger, true).await;
                        },
                        LedgerOrValidation::Validation(signed_validation) => {
                            info!("Own validation {:?} {}, fully validated {}",
                                signed_validation.validation.ledger_id,
                                signed_validation.validation.seq,
                                self.ledger_master.fully_validated);
                            self.ledger_master.add_validation(&signed_validation).await;
                        },
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

                        let (signed_validation, _) = self.to_acquire.pop_front().unwrap();
                        let pk = signed_validation.validation.node_id.clone();
                        let digest = signed_validation.validation.ledger_id.clone();

                        self.validation_dependencies.entry(digest.clone()).or_insert_with(Vec::new).push(signed_validation);
                        let entry = self.ledger_dependencies.get_mut(&digest);
                        match entry {
                            Some(_) => {},
                            None => {
                                // info!("Inserting {:?} into ledger_dependencies", digest);
                                self.ledger_dependencies.insert(digest.clone(), (Vec::new(), pk.clone()));
                                let address = self.committee
                                .primary(&pk)
                                .expect("Author is not in the committee")
                                .primary_to_primary;

                                let mut digests = vec![];
                                digests.push(digest);
                                let message = PrimaryPrimaryMessage::LedgerRequest(digests, self.name);
                                let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize batch sync request");
                                info!("Sending LedgerRequest to {:?} for ledger {:?}", pk, digest);
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                        }
                    }

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}