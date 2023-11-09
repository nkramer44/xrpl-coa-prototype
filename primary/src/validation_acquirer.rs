use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration, Instant};
use config::Committee;
use crypto::{Digest, PublicKey};
use network::SimpleSender;
use crate::{Ledger, SignedValidation};
use log::{info};
use tokio::sync::{Mutex, RwLock};
use crate::primary::{PrimaryPrimaryMessage};

const TIMER_RESOLUTION: u64 = 100;
const ACQUIRE_DELAY: u128 = 300;

fn clock() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}

pub struct ValidationAcquirer {
    committee: Committee,
    name: PublicKey,
    to_acquire: Arc<RwLock<VecDeque<(SignedValidation, u128)>>>,
    validation_dependencies: Arc<Mutex<HashMap<Digest, Vec<SignedValidation>>>>,
    ledger_dependencies: Arc<Mutex<HashMap<Digest, (Vec<Ledger>, PublicKey)>>>,
    network: SimpleSender,
}

impl ValidationAcquirer {
    pub fn spawn(
        committee: Committee,
        name: PublicKey,
        to_acquire: Arc<RwLock<VecDeque<(SignedValidation, u128)>>>,
        validation_dependencies: Arc<Mutex<HashMap<Digest, Vec<SignedValidation>>>>,
        ledger_dependencies: Arc<Mutex<HashMap<Digest, (Vec<Ledger>, PublicKey)>>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                name,
                to_acquire,
                validation_dependencies,
                ledger_dependencies,
                network: SimpleSender::new(),
            }
                .run()
                .await
        });
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                () = &mut timer => {
                    info!("VALIDATION WAITER TIMER");
                    let now = clock();
                    loop{
                        let mut acquire_read = self.to_acquire.read().await;
                        let f = acquire_read.front();
                        if f.is_none() {
                            break;
                        }

                        let (_, t) = f.unwrap();
                        if (now - t) < ACQUIRE_DELAY {
                            break;
                        }

                        drop(acquire_read);
                        let (signed_validation, _) = self.to_acquire.write().await.pop_front().unwrap();
                        let pk = signed_validation.validation.node_id.clone();
                        let digest = signed_validation.validation.ledger_id.clone();

                        self.validation_dependencies.lock().await.entry(digest.clone()).or_insert_with(Vec::new).push(signed_validation);
                        let mut write_lock = self.ledger_dependencies.lock().await;
                        let entry = write_lock.get_mut(&digest);
                        match entry {
                            Some(_) => {},
                            None => {
                                // info!("Inserting {:?} into ledger_dependencies", digest);
                                write_lock.insert(digest.clone(), (Vec::new(), pk.clone()));
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
                },
            }
        }
    }
}