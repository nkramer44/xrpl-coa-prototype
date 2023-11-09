use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::ops::Sub;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use log::{debug, error, info, warn};
use rand::RngCore;
use rand::rngs::OsRng;
use tokio::sync::mpsc::{Receiver, Sender};
use xrpl_consensus_core::{Ledger as LedgerTrait, LedgerIndex, NetClock};
use xrpl_consensus_validations::{Adaptor, ValidationError, ValidationParams, Validations};
use xrpl_consensus_validations::arena_ledger_trie::ArenaLedgerTrie;

use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use primary::{ConsensusPrimaryMessage, PrimaryConsensusMessage, Batches, SignedValidation, Validation};
use primary::Ledger;
use primary::proposal::{ConsensusRound, Proposal, SignedProposal};

use crate::adaptor::ValidationsAdaptor;

pub mod adaptor;

pub const INITIAL_WAIT: Duration = Duration::from_secs(2);
pub const MAX_PROPOSAL_SIZE: usize = 200_000;

pub enum ConsensusState {
    NotSynced,
    InitialWait(SystemTime),
    Deliberating,
    Executing,
}

pub struct Consensus {
    /// The UNL information.
    committee: Committee,
    node_id: PublicKey,
    /// The last `Ledger` we have validated.
    latest_ledger: Ledger,
    ///
    round: ConsensusRound,
    clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
    state: ConsensusState,
    proposals: HashMap<Digest, HashMap<PublicKey, Arc<SignedProposal>>>,
    batch_pool: VecDeque<(Digest, WorkerId)>,
    negative_pool: HashSet<(Digest, WorkerId)>,
    //TODO cleanup
    validations: Validations<ValidationsAdaptor, ArenaLedgerTrie<Ledger>>,
    validation_cookie: u64,
    signature_service: SignatureService,
    progress: HashMap<LedgerIndex, HashSet<PublicKey>>,
    freshness: HashMap<PublicKey, u32>,
    timer_count: u32,
    //TODO cleanup, safe and simple to clean if knows when ledgers are fully validated

    rx_primary: Receiver<PrimaryConsensusMessage>,
    rx_primary_data: Receiver<Batches>,
    tx_primary: Sender<ConsensusPrimaryMessage>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        node_id: PublicKey,
        signature_service: SignatureService,
        adaptor: ValidationsAdaptor,
        clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
        rx_primary: Receiver<PrimaryConsensusMessage>,
        rx_primary_data: Receiver<Batches>,
        tx_primary: Sender<ConsensusPrimaryMessage>,
    ) {
        tokio::spawn(async move {
            let mut rng = OsRng {};
            let now = clock.read().unwrap().now();
            Self {
                committee,
                node_id,
                latest_ledger: Ledger::make_genesis(),
                round: 0.into(),
                clock: clock.clone(),
                state: ConsensusState::InitialWait(now),
                proposals: Default::default(),
                batch_pool: VecDeque::new(),
                negative_pool: Default::default(),
                validations: Validations::new(ValidationParams::default(), adaptor, clock),
                validation_cookie: rng.next_u64(),
                signature_service,
                progress: HashMap::new(),
                freshness: HashMap::new(),
                timer_count: 0,
                rx_primary,
                rx_primary_data,
                tx_primary,
            }
                .run()
                .await;
        });
    }

    fn new(
        committee: Committee,
        node_id: PublicKey,
        signature_service: SignatureService,
        adaptor: ValidationsAdaptor,
        clock: Arc<RwLock<<ValidationsAdaptor as Adaptor>::ClockType>>,
        rx_primary: Receiver<PrimaryConsensusMessage>,
        rx_primary_data: Receiver<Batches>,
        tx_primary: Sender<ConsensusPrimaryMessage>,
    ) -> Self {
        let mut rng = OsRng {};
        let now = clock.read().unwrap().now();
        Self {
            committee,
            node_id,
            latest_ledger: Ledger::make_genesis(),
            round: 0.into(),
            clock: clock.clone(),
            state: ConsensusState::InitialWait(now),
            proposals: Default::default(),
            batch_pool: VecDeque::new(),
            negative_pool: Default::default(),
            validations: Validations::new(ValidationParams::default(), adaptor, clock),
            validation_cookie: rng.next_u64(),
            signature_service,
            progress: HashMap::new(),
            freshness: HashMap::new(),
            timer_count: 0,
            rx_primary,
            rx_primary_data,
            tx_primary,
        }
    }

    async fn run(&mut self) {
        let mut pks : Vec<PublicKey> = self.committee.authorities.iter().map(|(pk, _)| (*pk).clone()).collect();
        pks.into_iter().for_each(|(pk)| {
            self.freshness.insert(pk, 0);
        });

        loop {
            tokio::select! {
                Some(message) = self.rx_primary_data.recv() => {
                    //debug!("Received batches");
                    match message {
                        Batches::Batches(batches) => {
                            // Store any batches that come from the primary in batch_pool to be included
                            // in a future proposal.
                            for batch in batches {
                                if !self.batch_pool.contains(&batch) {
                                    self.batch_pool.push_front(batch);
                                }
                            }
                        },
                    }
                },

                Some(message) = self.rx_primary.recv() => {
                    match message {
                        PrimaryConsensusMessage::Timeout(c) => {
                            info!("Received Timeout event, count {}", c);
                            self.timer_count = c;
                            self.on_timeout().await;
                        },
                        PrimaryConsensusMessage::Proposal(proposal) => {
                            // info!("Received proposal: {:?}", proposal);
                            self.on_proposal_received(proposal);
                        },
                        PrimaryConsensusMessage::SyncedLedger(synced_ledger) => {
                            info!("Received SyncedLedger.");
                            self.validations.adaptor_mut().add_ledger(synced_ledger);
                        },
                        PrimaryConsensusMessage::Validation(validation) => {
                            // info!("Consensus Received validation : {:?}.", validation);
                            self.process_validation(validation).await;
                        },
                    }
                }
            }
        }
    }

    async fn on_timeout(&mut self) {
        if let Some((preferred_seq, preferred_id)) = self.validations.get_preferred(&self.latest_ledger) {
            info!("on_timeout, preferred ({:?} {}), latest ledger ({:?} {}) ", preferred_id, preferred_seq, self.latest_ledger.id, self.latest_ledger.seq);

            if preferred_id != self.latest_ledger.id() {
                if *self.latest_ledger.ancestors.last().unwrap() == preferred_id {
                    error!("We just switched to {:?}'s parent {:?}", self.latest_ledger.id, preferred_id);
                }
                warn!(
                    "Not on preferred ledger. We are on {:?} and preferred is {:?}",
                    (self.latest_ledger.id(), self.latest_ledger.seq()),
                    (preferred_id, preferred_seq)
                );

                //adjust batch pool
                //put back batches from latest proposal if have started deliberation
                match self.proposals.entry(self.latest_ledger.id) {
                    Entry::Occupied(mut proposals) => {
                        match proposals.get_mut().entry(self.node_id) {
                            Entry::Occupied(proposal) => {
                                info!("adjust pool, adding {:?} proposal batches", proposal.get().proposal.batches.len());
                                self.batch_pool.extend(proposal.get().proposal.batches.iter());
                            }
                            Entry::Vacant(_) => {}
                        }
                    }
                    Entry::Vacant(_) => {}
                };

                //adjust pool by adding batches on the wrong branch and removing from right branch
                //by current design (oct 31, 2023), the node can be off by one ledger, not more

                //put back batches in the ledgers on the wrong branch
                info!("adjust pool, adding {:?} ledger batches", self.latest_ledger.batch_set.len());
                self.batch_pool.extend(self.latest_ledger.batch_set.iter());

                //latest's parent is the common ancestor of the branches
                let common_ancestor = self.latest_ledger.ancestors.last().unwrap().clone();
                info!("adjust pool, common_ancestor {:?} ", common_ancestor);
                //find out batches in the ledgers on the right branch, and take them out
                let mut l = self.validations.adaptor_mut().acquire(&preferred_id).await
                    .expect("ValidationsAdaptor did not have a ledger in cache.");
                while l.id != common_ancestor {
                    info!("adjust pool, ledger on preferred branch {:?} ", l.id);
                    l.batch_set.iter().for_each(|x| {self.negative_pool.insert(x.clone());});
                    l = self.validations.adaptor_mut().acquire(&l.ancestors.last().unwrap()).await
                    .expect("ValidationsAdaptor did not have a ledger in cache.");
                }
                self.batch_pool = self.batch_pool.iter()
                    .filter(|b| { !self.negative_pool.contains(b) })
                    .map(|b| *b)
                    .collect();

                //TODO filter new batches with negative_pool
                //TODO clean the negative_pool

                self.latest_ledger = self.validations.adaptor_mut().acquire(&preferred_id).await
                    .expect("ValidationsAdaptor did not have preferred ledger in cache.");
                self.reset(&preferred_id, false);
            }
        }

        match self.state {
            ConsensusState::NotSynced => {
                info!("NotSynced. Doing nothing.");
                // do nothing
            }
            ConsensusState::Executing => {
                info!("Executing. Doing nothing.");
                // do nothing
            }
            ConsensusState::InitialWait(wait_start) => {
                info!("InitialWait. Checking if we should propose.");

                if self.now().duration_since(wait_start).unwrap() >= INITIAL_WAIT {
                    info!("We should propose so we are.");
                    // If we're in the InitialWait state and we've waited longer than the configured
                    // initial wait time, make a proposal.
                    self.propose_first().await;
                }

                // else keep waiting
            }
            ConsensusState::Deliberating => {
                info!("Deliberating. Reproposing.");
                self.re_propose().await;
            }
        }
    }

    fn now(&self) -> SystemTime {
        self.clock.read().unwrap().now()
    }

    async fn propose_first(&mut self) {
        self.state = ConsensusState::Deliberating;

        let batch_set: HashSet<(Digest, WorkerId)> = if self.batch_pool.len() > MAX_PROPOSAL_SIZE {
            self.batch_pool.drain(self.batch_pool.len() - MAX_PROPOSAL_SIZE..).collect()
        } else {
            self.batch_pool.drain(..).collect()
        };

        info!(
            "Proposing first batch set w len {:?}", batch_set.len()/*,
            Self::truncate_batchset(&batch_set)*/
        );
        self.propose(batch_set).await;
    }

    async fn re_propose(&mut self) {
        if self.check_consensus() {
            info!("We have consensus!");
            self.build_ledger().await;
        } else {
            //info!("We don't have consensus :(");
            // threshold is the percentage of UNL members who need to propose the same set of batches
            let threshold = self.round.threshold();
            //info!("Threshold: {:?}", threshold);
            // This is the number of UNL members who need to propose the same set of batches based
            // on the threshold percentage.
            let normal = self.get_normal_validators();
            let num_nodes_threshold = (normal.len() as f32 * threshold).ceil() as u32;
            //info!("Num nodes needed: {:?}", num_nodes_threshold);

            // we should have, otherwise we should call propose_first()
            let proposals = self.proposals.get(&self.latest_ledger.id).unwrap();

            let num_proposals_for_this_ledger = proposals.len();

            //TODO check rippled logic for reducing the number of proposals required

            if num_proposals_for_this_ledger < num_nodes_threshold as usize {
                info!("We don't have consensus :( and We don't have enough proposals for child of ledger {:?}, need {}, have {}. Deferring to next round.",
                    self.latest_ledger.id, num_nodes_threshold, num_proposals_for_this_ledger);
                return
            }else{
                info!("We don't have consensus :( but We have enough proposals for child of ledger {:?}, need {}, have {}.",
                    self.latest_ledger.id, num_nodes_threshold, num_proposals_for_this_ledger);
            }

            // This will build a HashMap of (Digest, WorkerId) -> number of validators that proposed it,
            // then filter that HashMap to the (Digest, WorkerId)s that have a count > num_nodes_threshold
            // and collect that into a new proposal set.
            let new_proposal_set: HashSet<(Digest, WorkerId)> = proposals.iter()
                .map(|v| v.1)
                .filter(|v| v.proposal.parent_id == self.latest_ledger.id &&
                normal.contains(&v.proposal.node_id))
                .flat_map(|v| v.proposal.batches.iter())
                .fold(HashMap::<(Digest, WorkerId), u32>::new(), |mut map, digest| {
                    *map.entry(*digest).or_default() += 1;
                    map
                })
                .into_iter()
                .filter(|(_, count)| *count >= num_nodes_threshold)
                .map(|(digest, _)| digest)
                .collect();

            // Any batches that were included in our last proposal that do not make it to the next
            // proposal will be put back into the batch pool. This prevents batches that have not
            // been synced yet from ever getting into a ledger.
            // let to_queue = proposals.get(&self.node_id).unwrap().proposal.batches.iter()
            //     .filter(|batch| !new_proposal_set.contains(batch))
            //     .collect::<Vec<&(Digest, WorkerId)>>();
            // info!("Requeuing {:?} batches", to_queue.len());
            // self.batch_pool.extend(to_queue);
            // info!(
            //     "Reproposing batch set w len: {:?}",
            //     new_proposal_set.len()
            // );
            self.propose(new_proposal_set).await;
        }
    }

    fn truncate_batchset(batch_set: &HashSet<(Digest, WorkerId)>) -> Vec<String> {
        let mut trunc_batch_set: Vec<String> = batch_set.iter()
            .map(|(digest, _)| base64::encode(&digest.0[..5]))
            .collect();
        trunc_batch_set.sort();
        trunc_batch_set
    }

    async fn propose(&mut self, batch_set: HashSet<(Digest, WorkerId)>) {
        let proposal = Proposal::new(
            self.round,
            self.latest_ledger.id(),
            self.latest_ledger.seq() + 1,
            batch_set,
            self.node_id,
        );

        // info!("Proposing              {:?}", proposal);
        let signed_proposal = proposal.sign(&mut self.signature_service).await;
        //self.proposals.insert(self.node_id, signed_proposal.clone());
        self.on_proposal_received(signed_proposal.clone());
        self.tx_primary.send(ConsensusPrimaryMessage::Proposal(Arc::new(signed_proposal))).await
            .expect("Could not send proposal to primary.");
        self.round.next();
    }

    fn on_proposal_received(&mut self, proposal: SignedProposal) {
        info!("Received new proposal: {:?}", proposal);
        *self.freshness.get_mut(&proposal.proposal.node_id).unwrap() = self.timer_count;
        // The Primary will check the signature and make sure the proposal comes from
        // someone in our UNL before sending it to Consensus, therefore we do not need to
        // check here again. Additionally, the Primary will delay sending us a proposal until
        // it has synced all of the batches that it does not have in its local storage.
        let parent_ledger = proposal.proposal.parent_id.clone();
        match self.proposals.entry(parent_ledger) {
            Entry::Occupied(mut outer) => {
                match outer.get_mut().entry(proposal.proposal.node_id) {
                    Entry::Occupied(mut e) => {
                        if e.get().proposal.round < proposal.proposal.round {
                            e.insert(Arc::new(proposal));
                        }
                    }
                    Entry::Vacant(e) => {
                        e.insert(Arc::new(proposal));
                    }
                }
            }
            Entry::Vacant(mut outer) => {
                let mut new_proposals = HashMap::new();
                new_proposals.insert(proposal.proposal.node_id, Arc::new(proposal));
                outer.insert(new_proposals);
            }
        }
    }

    fn get_normal_validators<'a>(&'a self) -> HashSet<&'a PublicKey> {
        let mut too_fast: HashSet<&PublicKey> = HashSet::new();
        self.progress.iter()
            .filter(|(r, _)| **r > self.latest_ledger.seq)
            .for_each(|(_, pks)| too_fast.extend(pks.iter()));

        let too_slow: HashSet<&PublicKey> = if self.timer_count > 20 {
            let slow_cut_off = self.timer_count - 20;//TODO 20 seconds?
            self.freshness.iter()
                .filter(|&(_, &last_seen)| last_seen <= slow_cut_off)
                .map(|(pk, _)| pk).collect()
        } else {
            HashSet::new()
        };

        let normal : HashSet<&PublicKey> = self.committee.authorities.iter()
            .filter(|(pk, _)| !(too_fast.contains(pk) || too_slow.contains(pk)))
            .map(|(pk, _)| pk).collect();
        normal
    }

    fn check_consensus(&self) -> bool {
        // Find our proposal
        match self.proposals.get(&self.latest_ledger.id) {
            Some(proposals) => {
                let our_proposal = proposals.get(&self.node_id)
                    .expect("We did not propose anything the first round.");

                // Determine the number of nodes that need to agree with our proposal to reach consensus
                // by multiplying the number of validators in our UNL by 0.80 and taking the ceiling.
                let normal = self.get_normal_validators();
                let num_nodes_for_threshold = (normal.len() as f32 * 0.80).ceil() as usize;

                // Determine how many proposals have the same set of batches as us.
                let num_matching_sets = proposals.iter()
                    .filter(|(pk, prop)| normal.contains(pk) && prop.proposal.batches == our_proposal.proposal.batches)
                    .count();

                // If 80% or more of UNL nodes proposed the same batch set, we have reached consensus,
                // otherwise we need another round.
                info!("check_consensus, same as ours {}, quorum {}", num_matching_sets, num_nodes_for_threshold);
                let great = num_matching_sets >= num_nodes_for_threshold;
                if !great{
                    for (_, p) in proposals {
                        info!("{:?}", p);
                    }
                }
                great
            } ,
            None => false,
        }
    }

    async fn build_ledger(&mut self) {
        self.state = ConsensusState::Executing;

        let new_ledger = self.execute();

        let validation = Validation::new(
            new_ledger.seq(),
            new_ledger.id(),
            self.clock.read().unwrap().now(),
            self.clock.read().unwrap().now(),
            self.node_id,
            self.node_id,
            true,
            true,
            self.validation_cookie,
        );

        let signed_validation = validation.sign(&mut self.signature_service).await;

        // Need to add the new ledger to our cache before adding it to self.validations because
        // self.validations.try_add will call Adaptor::acquire, which needs to have the ledger
        // in its cache or else it will panic.
        self.validations.adaptor_mut().add_ledger(new_ledger.clone());

        info!("About to add our own validation for {:?}", new_ledger.id);
        if let Err(e) = self.validations.try_add(&self.node_id, &signed_validation).await {
            match e {
                ValidationError::ConflictingSignTime(e) => {
                    error!("{:?} could not be added due to different sign times. \
                    This could happen if we build a ledger with no batches then mistakenly switch\
                     to the current ledger's parent during branch selection and then build another \
                     ledger with no batches based on the parent because the hashes of both ledgers \
                     will be the same. Error.", signed_validation);
                }
                _ => { error!("{:?} could not be added. Error: {:?}", signed_validation, e); }
            }
            return;
        }

        self.tx_primary.send(ConsensusPrimaryMessage::Validation(signed_validation)).await
            .expect("Failed to send validation to Primary.");

        let parent_id = self.latest_ledger.id;
        self.latest_ledger = new_ledger;

        self.tx_primary.send(ConsensusPrimaryMessage::NewLedger(self.latest_ledger.clone())).await
            .expect("Failed to send new ledger to Primary.");

        self.reset(&parent_id, true);

        info!("Did a new ledger {:?}. Num Batches {:?}", (self.latest_ledger.id, self.latest_ledger.seq()), self.latest_ledger.batch_set.len());

        // #[cfg(feature = "benchmark")]
        // for (batch, _) in &self.latest_ledger.batch_set {
        //     if *batch.0.get(0).unwrap() == 0 as u8 {
        //         info!("Committed {:?} ", batch);
        //     }
        // }
    }

    fn execute(&self) -> Ledger {
        let mut new_ancestors = self.latest_ledger.ancestors.clone();
        new_ancestors.push(self.latest_ledger.id());
        //assert!(new_ancestors[0] == self.latest_ledger.id());
        /*let mut new_ancestors = vec![self.latest_ledger.id()];
        new_ancestors.extend_from_slice(self.latest_ledger.ancestors.as_slice());*/
        //TODO assuming we proposed
        let our_proposal = self.proposals.get(&self.latest_ledger.id).unwrap().get(&self.node_id)
            .expect("Could not find our own proposal");

        // TODO: Do we need to store a Vec<Digest> in Ledger and sort batches here so that they
        //  yield the same ID on every validator?
        // let batches = our_proposal.proposal.batches.iter()
        //     .map(|b| b.0)
        //     .collect();
        Ledger::new(
            self.latest_ledger.seq() + 1,
            new_ancestors,
            our_proposal.proposal.batches.clone(),
        )
    }

    fn reset(&mut self, parent: &Digest, waiting: bool) {
        self.proposals.remove(parent);
        self.round.reset();
        let t = if waiting { self.now() } else { self.now().sub(INITIAL_WAIT) };
        self.state = ConsensusState::InitialWait(t);
    }

    async fn process_validation(&mut self, validation: SignedValidation) {
        info!("Received validation from {:?} for ({:?}, {:?})", validation.validation.node_id, validation.validation.ledger_id, validation.validation.seq);
        *self.freshness.get_mut(&validation.validation.node_id).unwrap() = self.timer_count;
        self.progress.entry(validation.validation.seq).or_insert_with(HashSet::new).insert(validation.validation.node_id);
        if let Err(e) = self.validations.try_add(&validation.validation.node_id, &validation).await {
            error!("{:?} could not be added. Error: {:?}", validation, e);
        }
    }
}

//
// #[cfg(test)]
// mod tests {
//     use std::iter::FromIterator;
//     use env_logger::Env;
//     use tokio::sync::mpsc::channel;
//     use xrpl_consensus_core::WallNetClock;
//
//     use config::{Import, KeyPair};
//     use crypto::Hash;
//
//     use super::*;
//
//     #[tokio::test]
//     async fn test_branch_selection_selecting_parent() {
//         let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or("info"));
//
//         logger.init();
//
//         let keypair = KeyPair::new();
//         let keypair2 = KeyPair::new();
//         let clock = Arc::new(RwLock::new(WallNetClock));
//         let (tx_primary_consensus, rx_primary_consensus) = channel(1000);
//         let (tx_consensus_primary, rx_consensus_primary) = channel(1000);
//         let mut sig_service = SignatureService::new(keypair.secret);
//         let mut consensus = Consensus::new(
//             Committee::import("/Users/nkramer/Documents/dev/nk/xrpl-coa-prototype/benchmark/.committee.json").unwrap(),
//             keypair.name,
//             sig_service.clone(),
//             ValidationsAdaptor::new(clock.clone()),
//             clock.clone(),
//             rx_primary_consensus,
//             tx_consensus_primary,
//         );
//
//         consensus.proposals.insert(keypair.name.clone(), Arc::new(
//             Proposal::new(
//                 ConsensusRound::from(1),
//                 Ledger::make_genesis().id,
//                 2,
//                 HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
//                 keypair.name,
//             ).sign(&mut sig_service).await
//         ));
//
//         consensus.build_ledger().await;
//
//         consensus.process_validation(
//             Validation::new(
//                 2,
//                 consensus.latest_ledger.id,
//                 clock.read().unwrap().now(),
//                 clock.read().unwrap().now(),
//                 keypair2.name,
//                 keypair2.name,
//                 true,
//                 true,
//                 1,
//             ).sign(&mut SignatureService::new(keypair2.secret)).await
//         ).await;
//
//         consensus.proposals.insert(keypair.name.clone(), Arc::new(
//             Proposal::new(
//                 ConsensusRound::from(1),
//                 consensus.latest_ledger.id,
//                 3,
//                 HashSet::from_iter(vec![([1u8].as_slice().digest(), 1)].into_iter()),
//                 keypair.name,
//             ).sign(&mut sig_service).await
//         ));
//
//         consensus.build_ledger().await;
//
//         consensus.on_timeout().await;
//         /*tx_primary_consensus.send(PrimaryConsensusMessage::Timeout).await.expect("");
//         tx_primary_consensus.send(PrimaryConsensusMessage::Proposal(
//             Proposal::new(
//                 ConsensusRound::from(1),
//                 Ledger::make_genesis().id,
//                 2,
//                 HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
//                 keypair2.name
//             ).sign(&mut sig_service).await
//         )).await.expect("TODO: panic message");
//
//         tx_primary_consensus.send(PrimaryConsensusMessage::Proposal(
//             Proposal::new(
//                 ConsensusRound::from(1),
//                 Ledger::make_genesis().id,
//                 2,
//                 HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
//                 keypair3.name
//             ).sign(&mut sig_service).await
//         )).await.expect("TODO: panic message");*/
//     }
// }
