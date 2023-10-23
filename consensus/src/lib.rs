use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use log::{error, info, warn};
use rand::RngCore;
use rand::rngs::OsRng;
use tokio::sync::mpsc::{Receiver, Sender};
use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock};
use xrpl_consensus_validations::{Adaptor, ValidationError, ValidationParams, Validations};
use xrpl_consensus_validations::arena_ledger_trie::ArenaLedgerTrie;

use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey, SignatureService};
use primary::{ConsensusPrimaryMessage, PrimaryConsensusMessage, SignedValidation, Validation};
use primary::Ledger;
use primary::proposal::{ConsensusRound, Proposal, SignedProposal};

use crate::adaptor::ValidationsAdaptor;

pub mod adaptor;

pub const INITIAL_WAIT: Duration = Duration::from_secs(2);
pub const MAX_PROPOSAL_SIZE: usize = 100_000;

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
    proposals: HashMap<PublicKey, Arc<SignedProposal>>,
    batch_pool: VecDeque<(Digest, WorkerId)>,
    validations: Validations<ValidationsAdaptor, ArenaLedgerTrie<Ledger>>,
    validation_cookie: u64,
    signature_service: SignatureService,

    rx_primary: Receiver<PrimaryConsensusMessage>,
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
                validations: Validations::new(ValidationParams::default(), adaptor, clock),
                validation_cookie: rng.next_u64(),
                signature_service,
                rx_primary,
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
            validations: Validations::new(ValidationParams::default(), adaptor, clock),
            validation_cookie: rng.next_u64(),
            signature_service,
            rx_primary,
            tx_primary,
        }
    }

    async fn run(&mut self) {
        while let Some(message) = self.rx_primary.recv().await {
            match message {
                PrimaryConsensusMessage::Timeout => {
                    info!("Received Timeout event.");
                    self.on_timeout().await;
                }
                PrimaryConsensusMessage::Batch(batch) => {
                    // Store any batches that come from the primary in batch_pool to be included
                    // in a future proposal.
                    // info!("Received batch {:?}.", batch.0);
                    if !self.batch_pool.contains(&batch) {
                        self.batch_pool.push_front(batch);
                    }
                }
                PrimaryConsensusMessage::Proposal(proposal) => {
                    // info!("Received proposal: {:?}", proposal);
                    self.on_proposal_received(proposal);
                }
                PrimaryConsensusMessage::SyncedLedger(synced_ledger) => {
                    info!("Received SyncedLedger {:?}.", synced_ledger.id);
                    self.validations.adaptor_mut().add_ledger(synced_ledger);
                }
                PrimaryConsensusMessage::Validation(validation) => {
                    // info!("Consensus Received validation : {:?}.", validation);
                    self.process_validation(validation).await;
                }
            }
        }
    }

    async fn on_timeout(&mut self) {
        if let Some((preferred_seq, preferred_id)) = self.validations.get_preferred(&self.latest_ledger) {
            if preferred_id != self.latest_ledger.id() {
                if self.latest_ledger.ancestors[0] == preferred_id {
                    error!("We just switched to {:?}'s parent {:?}", self.latest_ledger.id, preferred_id);
                }
                warn!(
                    "Not on preferred ledger. We are on {:?} and preferred is {:?}",
                    (self.latest_ledger.id(), self.latest_ledger.seq()),
                    (preferred_id, preferred_seq)
                );

                self.latest_ledger = self.validations.adaptor_mut().acquire(&preferred_id).await
                    .expect("ValidationsAdaptor did not have preferred ledger in cache.");
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

                if self.now().duration_since(wait_start).unwrap() > INITIAL_WAIT {
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

        let mut batch_set: Vec<(Digest, WorkerId)> = if self.batch_pool.len() > MAX_PROPOSAL_SIZE {
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
        match self.check_consensus() {
            Some(batches) => {
                info!("We have consensus!");
                self.build_ledger(batches).await;
            }
            None => {
                info!("We don't have consensus :(");
                // threshold is the percentage of UNL members who need to propose the same set of batches
                let threshold = self.round.threshold();
                info!("Threshold: {:?}", threshold);
                // This is the number of UNL members who need to propose the same set of batches based
                // on the threshold percentage.
                let num_nodes_threshold = (self.committee.authorities.len() as f32 * threshold).ceil() as u32;
                info!("Num nodes needed: {:?}", num_nodes_threshold);

                let num_proposals_for_this_ledger = self.proposals.iter()
                    .map(|p| p.1)
                    .filter(|p| p.proposal.parent_id == self.latest_ledger.id)
                    // .filter(|p| p.proposal.round == self.round - 1)
                    .count();
                info!("We have {:?} proposals for ledger {:?}", num_proposals_for_this_ledger, self.latest_ledger.id);

                if num_proposals_for_this_ledger < num_nodes_threshold as usize {
                    info!("We don't have enough proposals for child of ledger {:?}. Deferring to next round.", self.latest_ledger.id);
                    return
                }

                // This will build a HashMap of (Digest, WorkerId) -> number of validators that proposed it,
                // then filter that HashMap to the (Digest, WorkerId)s that have a count > num_nodes_threshold
                // and collect that into a new proposal set.
                let mut new_proposal_set: Vec<(Digest, WorkerId)> = self.proposals.iter()
                    .map(|v| v.1)
                    .filter(|v| v.proposal.parent_id == self.latest_ledger.id)
                    .flat_map(|v| v.proposal.batches.iter())
                    .fold(HashMap::<(Digest, WorkerId), u32>::new(), |mut map, digest| {
                        *map.entry(*digest).or_default() += 1;
                        map
                    })
                    .into_iter()
                    .filter(|(_, count)| *count > num_nodes_threshold)
                    .map(|(digest, _)| digest)
                    .collect();

                // Any batches that were included in our last proposal that do not make it to the next
                // proposal will be put back into the batch pool. This prevents batches that have not
                // been synced yet from ever getting into a ledger.
                let to_queue = self.proposals.get(&self.node_id).unwrap().proposal.batches.iter()
                    .filter(|batch| !new_proposal_set.contains(batch))
                    .collect::<Vec<&(Digest, WorkerId)>>();
                info!("Requeuing {:?} batches", to_queue.len());
                self.batch_pool.extend(
                    self.proposals.get(&self.node_id).unwrap().proposal.batches.iter()
                        .filter(|batch| !new_proposal_set.contains(batch))
                );
                // let trunc_batch_set = Self::truncate_batchset(&new_proposal_set);
                info!(
                "Reproposing batch set w len: {:?}",
                new_proposal_set.len()
            );
                self.propose(new_proposal_set).await;
            }
        }
        /*if self.check_consensus() {
            info!("We have consensus!");
            self.build_ledger().await;
        } else {
            info!("We don't have consensus :(");
            // threshold is the percentage of UNL members who need to propose the same set of batches
            let threshold = self.round.threshold();
            info!("Threshold: {:?}", threshold);
            // This is the number of UNL members who need to propose the same set of batches based
            // on the threshold percentage.
            let num_nodes_threshold = (self.committee.authorities.len() as f32 * threshold).ceil() as u32;
            info!("Num nodes needed: {:?}", num_nodes_threshold);

            let num_proposals_for_this_ledger = self.proposals.iter()
                .map(|p| p.1)
                .filter(|p| p.proposal.parent_id == self.latest_ledger.id)
                // .filter(|p| p.proposal.round == self.round - 1)
                .count();
            info!("We have {:?} proposals for ledger {:?}", num_proposals_for_this_ledger, self.latest_ledger.id);

            if num_proposals_for_this_ledger < num_nodes_threshold as usize {
                info!("We don't have enough proposals for child of ledger {:?}. Deferring to next round.", self.latest_ledger.id);
                return
            }

            // This will build a HashMap of (Digest, WorkerId) -> number of validators that proposed it,
            // then filter that HashMap to the (Digest, WorkerId)s that have a count > num_nodes_threshold
            // and collect that into a new proposal set.
            let new_proposal_set: HashSet<(Digest, WorkerId)> = self.proposals.iter()
                .map(|v| v.1)
                .filter(|v| v.proposal.parent_id == self.latest_ledger.id)
                .flat_map(|v| v.proposal.batches.iter())
                .fold(HashMap::<(Digest, WorkerId), u32>::new(), |mut map, digest| {
                    *map.entry(*digest).or_default() += 1;
                    map
                })
                .into_iter()
                .filter(|(_, count)| *count > num_nodes_threshold)
                .map(|(digest, _)| digest)
                .collect();

            // Any batches that were included in our last proposal that do not make it to the next
            // proposal will be put back into the batch pool. This prevents batches that have not
            // been synced yet from ever getting into a ledger.
            let to_queue = self.proposals.get(&self.node_id).unwrap().proposal.batches.iter()
                .filter(|batch| !new_proposal_set.contains(batch))
                .collect::<Vec<&(Digest, WorkerId)>>();
            info!("Requeuing {:?} batches", to_queue.len());
            self.batch_pool.extend(
                self.proposals.get(&self.node_id).unwrap().proposal.batches.iter()
                    .filter(|batch| !new_proposal_set.contains(batch))
            );
            // let trunc_batch_set = Self::truncate_batchset(&new_proposal_set);
            info!(
                "Reproposing batch set w len: {:?}",
                new_proposal_set.len()
            );
            self.propose(new_proposal_set).await;
        }*/
    }

    fn truncate_batchset(batch_set: &HashSet<(Digest, WorkerId)>) -> Vec<String> {
        let mut trunc_batch_set: Vec<String> = batch_set.iter()
            .map(|(digest, _)| base64::encode(&digest.0[..5]))
            .collect();
        trunc_batch_set.sort();
        trunc_batch_set
    }

    async fn propose(&mut self, mut batch_set: Vec<(Digest, WorkerId)>) {
        batch_set.sort();
        let proposal = Proposal::new(
            self.round,
            self.latest_ledger.id(),
            self.latest_ledger.seq() + 1,
            batch_set,
            self.node_id,
        );

        // info!("Proposing              {:?}", proposal);
        let signed_proposal = Arc::new(proposal.sign(&mut self.signature_service).await);
        self.proposals.insert(self.node_id, signed_proposal.clone());
        self.tx_primary.send(ConsensusPrimaryMessage::Proposal(signed_proposal)).await
            .expect("Could not send proposal to primary.");
        self.round.next();
    }

    fn on_proposal_received(&mut self, proposal: SignedProposal) {
        info!("Received new proposal: {:?}", (proposal.proposal.node_id, proposal.proposal.parent_id, proposal.proposal.round, proposal.proposal.batches.len()));
        // The Primary will check the signature and make sure the proposal comes from
        // someone in our UNL before sending it to Consensus, therefore we do not need to
        // check here again. Additionally, the Primary will delay sending us a proposal until
        // it has synced all of the batches that it does not have in its local storage.
        if proposal.proposal.parent_id == self.latest_ledger.id() {
            // Either insert the proposal if we haven't seen a proposal from this node,
            // or update an existing node's proposal if the given proposal's round is higher
            // than what we have in our map.
            match self.proposals.entry(proposal.proposal.node_id) {
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
    }

    fn check_consensus(&self) -> Option<Vec<(Digest, WorkerId)>> {
       /* info!("Checking for consensus.");
        let num_nodes_for_threshold = (self.committee.authorities.len() as f32 * 0.80).ceil() as u32;
        info!("Need {:?} nodes to agree.", num_nodes_for_threshold);
        let proposals_found: Vec<(Digest, u32)> = self.proposals.iter()
            .fold(HashMap::<Digest, u32>::new(), |mut map, (_, proposal)| {
                let digest = proposal.proposal.compute_batches_id();
                *map.entry(digest).or_default() += 1;
                map
            })
            .into_iter()
            .filter(|e| e.1 >= num_nodes_for_threshold)
            .collect();
        if proposals_found.is_empty() {
            info!("No consensus proposals found.");
            return None;
        } else {
            if proposals_found.len() > 1 {
                panic!("Multiple proposals reached consensus quorum.")
            } else {
                let consensus_proposal = proposals_found[0].0;
                let our_proposal = self.proposals.get(&self.node_id)
                    .expect("We did not propose anything the first round.");
                return if consensus_proposal != our_proposal.proposal.compute_batches_id() {
                    info!("The network agreed on a set of batches that were different than mine. Executing their ledger.");
                    let p = self.proposals.iter()
                        .find(|prop| prop.1.proposal.compute_batches_id() == consensus_proposal)
                        .unwrap();
                    Some(p.1.proposal.batches.clone())
                } else {
                    Some(our_proposal.proposal.batches.clone())
                }
            }
        }*/
        // Find our proposal
        let our_proposal = self.proposals.get(&self.node_id)
            .expect("We did not propose anything the first round.");

        // Determine the number of nodes that need to agree with our proposal to reach consensus
        // by multiplying the number of validators in our UNL by 0.80 and taking the ceiling.
        let num_nodes_for_threshold = (self.committee.authorities.len() as f32 * 0.80).ceil() as usize;

        // Determine how many proposals have the same set of batches as us.
        let num_matching_sets = self.proposals.iter()
            .filter(|p| p.1.proposal.batches == our_proposal.proposal.batches)
            .count();

        // If 80% or more of UNL nodes proposed the same batch set, we have reached consensus,
        // otherwise we need another round.
        return if num_matching_sets >= num_nodes_for_threshold {
            Some(our_proposal.proposal.batches.clone())
        } else {
            None
        }
    }

    async fn build_ledger(&mut self, batches: Vec<(Digest, WorkerId)>) {
        self.state = ConsensusState::Executing;

        let new_ledger = self.execute(batches);

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

        self.latest_ledger = new_ledger;

        self.tx_primary.send(ConsensusPrimaryMessage::NewLedger(self.latest_ledger.clone())).await
            .expect("Failed to send new ledger to Primary.");

        self.reset();

        info!("Did a new ledger {:?}. Num Batches {:?}", (self.latest_ledger.id, self.latest_ledger.seq()), self.latest_ledger.batch_set.len());

        /*#[cfg(feature = "benchmark")]
        for batch in &self.latest_ledger.batch_set {
            info!("Committed {:?} ", batch);
        }*/
    }

    fn execute(&self, batches: Vec<(Digest, WorkerId)>) -> Ledger {
        let mut new_ancestors = vec![self.latest_ledger.id()];
        new_ancestors.extend_from_slice(self.latest_ledger.ancestors.as_slice());

        // TODO: Do we need to store a Vec<Digest> in Ledger and sort batches here so that they
        //  yield the same ID on every validator?
        let batches = batches.iter()
            .map(|b| b.0)
            .collect();
        Ledger::new(
            self.latest_ledger.seq() + 1,
            new_ancestors,
            batches,
        )
    }

    fn reset(&mut self) {
        self.proposals.clear();
        self.round.reset();
        self.state = ConsensusState::InitialWait(self.now());
    }

    async fn process_validation(&mut self, validation: SignedValidation) {
        info!("Received validation from {:?} for ({:?}, {:?})", validation.validation.node_id, validation.validation.ledger_id, validation.validation.seq);
        if let Err(e) = self.validations.try_add(&validation.validation.node_id, &validation).await {
            error!("{:?} could not be added. Error: {:?}", validation, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;
    use env_logger::Env;
    use tokio::sync::mpsc::channel;
    use xrpl_consensus_core::WallNetClock;

    use config::{Import, KeyPair};
    use crypto::Hash;

    use super::*;

    #[tokio::test]
    async fn test_branch_selection_selecting_parent() {
        /*let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or("info"));

        logger.init();

        let keypair = KeyPair::new();
        let keypair2 = KeyPair::new();
        let clock = Arc::new(RwLock::new(WallNetClock));
        let (tx_primary_consensus, rx_primary_consensus) = channel(1000);
        let (tx_consensus_primary, rx_consensus_primary) = channel(1000);
        let mut sig_service = SignatureService::new(keypair.secret);
        let mut consensus = Consensus::new(
            Committee::import("/Users/nkramer/Documents/dev/nk/xrpl-coa-prototype/benchmark/.committee.json").unwrap(),
            keypair.name,
            sig_service.clone(),
            ValidationsAdaptor::new(clock.clone()),
            clock.clone(),
            rx_primary_consensus,
            tx_consensus_primary,
        );

        consensus.proposals.insert(keypair.name.clone(), Arc::new(
            Proposal::new(
                ConsensusRound::from(1),
                Ledger::make_genesis().id,
                2,
                HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
                keypair.name,
            ).sign(&mut sig_service).await
        ));

        consensus.build_ledger().await;

        consensus.process_validation(
            Validation::new(
                2,
                consensus.latest_ledger.id,
                clock.read().unwrap().now(),
                clock.read().unwrap().now(),
                keypair2.name,
                keypair2.name,
                true,
                true,
                1,
            ).sign(&mut SignatureService::new(keypair2.secret)).await
        ).await;

        consensus.proposals.insert(keypair.name.clone(), Arc::new(
            Proposal::new(
                ConsensusRound::from(1),
                consensus.latest_ledger.id,
                3,
                HashSet::from_iter(vec![([1u8].as_slice().digest(), 1)].into_iter()),
                keypair.name,
            ).sign(&mut sig_service).await
        ));

        consensus.build_ledger().await;

        consensus.on_timeout().await;*/
        /*tx_primary_consensus.send(PrimaryConsensusMessage::Timeout).await.expect("");
        tx_primary_consensus.send(PrimaryConsensusMessage::Proposal(
            Proposal::new(
                ConsensusRound::from(1),
                Ledger::make_genesis().id,
                2,
                HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
                keypair2.name
            ).sign(&mut sig_service).await
        )).await.expect("TODO: panic message");

        tx_primary_consensus.send(PrimaryConsensusMessage::Proposal(
            Proposal::new(
                ConsensusRound::from(1),
                Ledger::make_genesis().id,
                2,
                HashSet::from_iter(vec![([0u8].as_slice().digest(), 1)].into_iter()),
                keypair3.name
            ).sign(&mut sig_service).await
        )).await.expect("TODO: panic message");*/
    }
}