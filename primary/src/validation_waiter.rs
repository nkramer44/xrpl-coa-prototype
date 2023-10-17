// Copyright(C) Facebook, Inc. and its affiliates.
use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use async_recursion::async_recursion;
use bytes::Bytes;
use log::{error, info};
// use futures::stream::futures_unordered::FuturesUnordered;
// use futures::stream::StreamExt as _;
// use log::error;
// use serde::Serialize;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use xrpl_consensus_core::Validation;
use config::Committee;
use crypto::{Digest, PublicKey};
use network::SimpleSender;
use crate::{Ledger, SignedValidation};
use crate::primary::PrimaryPrimaryMessage;

const TIMER_RESOLUTION: u64 = 100;
const ACQUIRE_DELAY: u128 = 300;

fn clock() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}

// #[derive(Debug)]
// pub enum ToWaiterMessage {
//     NetworkValidation(SignedValidation),
//     NetworkLedger(Ledger),
// }

/// Waits to receive all the ancestors of a Validation before looping it back to the `Core`
/// for further processing.
pub struct ValidationWaiter {
    name: PublicKey,
    committee: Committee,
    store: Store,
    rx_network_validations: Receiver<SignedValidation>,
    rx_network_ledgers: Receiver<Ledger>,
    tx_loopback_validations: Sender<SignedValidation>,
    tx_loopback_ledgers: Sender<Ledger>,
    rx_own_ledgers: Receiver<Ledger>,
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
        tx_loopback_validations: Sender<SignedValidation>,
        tx_loopback_ledgers: Sender<Ledger>,
        rx_own_ledgers: Receiver<Ledger>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                rx_network_validations,
                rx_network_ledgers,
                tx_loopback_validations,
                tx_loopback_ledgers,
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
                    self.tx_loopback_validations.send(signed_validation).await.expect("TODO: panic message");
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
            self.tx_loopback_validations.send(v).await.expect("TODO: panic message");
        }
    }

    #[async_recursion]
    async fn store_children(&mut self, parent_id: &Digest) {
        match self.ledger_dependencies.remove(parent_id) {
            Some((children, _)) => {
                for ledger in children.into_iter() {
                    self.store_ledger(ledger, false).await;

                    // self.store.write(ledger.id.to_vec(), bincode::serialize(&ledger).unwrap()).await;
                    // self.tx_loopback_ledgers.send(ledger.clone()).await.expect("TODO: panic message");
                    // self.try_deliver(&ledger.id).await;
                    // self.store_children(&ledger.id).await;
                }
            }
            None => {}
        }
    }

    async fn store_ledger(&mut self, ledger: Ledger, own : bool){
        self.store.write(ledger.id.to_vec(), bincode::serialize(&ledger).unwrap()).await;
        let lid = ledger.id.clone();
        if ! own {
            self.tx_loopback_ledgers.send(ledger).await.expect("TODO: panic message");
            //TODO deliver ledger in the same channel as validations so that ledgers are always delivered before the validations!!!
        }
        self.try_deliver(&lid).await;
        self.store_children(&lid).await;
    }

    async fn try_store_ledger(&mut self, ledger: Ledger) -> Option<(Digest, PublicKey)> {
        if ledger.ancestors.is_empty() || self.ledger_dependencies.get(&ledger.id).is_none() {
            return None;
        }

        let parent = ledger.ancestors[0].clone();
        match self.store.read(parent.to_vec()).await {
            Ok(Some(_)) => {
                self.store_ledger(ledger, false).await;
                // self.store.write(ledger.id.to_vec(), bincode::serialize(&ledger).unwrap()).await;
                // self.tx_loopback_ledgers.send(ledger.clone()).await.expect("TODO: panic message");
                // self.try_deliver(&ledger.id).await;
                // self.store_children(&ledger.id).await;
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
                // match entry {
                //     Some((ledgers, _)) => ledgers.push(ledger),
                //     None => self.ledger_dependencies.insert(parent.clone(), (Vec[ledger], pk.clone())),
                // };
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

        // let acquire = async move | digest : Digest, pk : PublicKey | {
        //     let address = self.committee
        //         .primary(&pk)
        //         .expect("Author is not in the committee")
        //         .primary_to_primary;
        //     let message = PrimaryPrimaryMessage::LedgerRequest(Vec[digest],pk);
        //     let bytes = bincode::serialize(&message)
        //         .expect("Failed to serialize batch sync request");
        //     self.network.send(address, Bytes::from(bytes)).await;
        // };

        loop {

            tokio::select! {
                Some(signed_validation) = self.rx_network_validations.recv() => {
                    //TODO verify sig
                    info!("Waiter Received validation for ledger {:?}", signed_validation.validation.ledger_id);
                    let ledger_id = signed_validation.validation.ledger_id;
                    match self.store.read(ledger_id.to_vec()).await{
                        Ok(Some(_)) => {
                            self.tx_loopback_validations
                            .send(signed_validation)
                            .await //TODO need to wait?
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
                    match self.try_store_ledger(ledger).await {
                        Some((digest, pk)) => {
                            // acquire(digest, pk).await;
                            let address = self.committee
                                .primary(&pk)
                                .expect("Author is not in the committee")
                                .primary_to_primary;
                            let mut digests = vec![];
                            digests.push(digest);
                            let message = PrimaryPrimaryMessage::LedgerRequest(digests,pk);
                            let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize batch sync request");
                            self.network.send(address, Bytes::from(bytes)).await;
                        },
                        None => {}
                    }
                },

                Some(ledger) = self.rx_own_ledgers.recv() => {
                    //TODO check parent exist in DB
                    info!("Waiter got our own ledger {:?}", ledger.id);
                    self.store_ledger(ledger, true).await;
                    // self.store.write(ledger.id.to_vec(), bincode::serialize(&ledger).unwrap()).await;
                    // self.try_deliver(&ledger.id).await;
                    // self.store_children(&ledger.id).await;
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
                                let message = PrimaryPrimaryMessage::LedgerRequest(digests,pk);
                                let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize batch sync request");
                                self.network.send(address, Bytes::from(bytes)).await;
                                //                                        acquire(digest, pk).await;
                            }
                        }
                    }

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }
        }
    }
}
//
// TODO Cleanup internal state.
// TODO retry