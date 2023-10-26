use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use log::{error, info};
//use std::path::Ancestors;
use serde::{Deserialize, Serialize};

use xrpl_consensus_core::LedgerIndex;
//use config::WorkerId;

use crypto::{Digest, Hash};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Ledger {
    pub id: Digest,
    seq: LedgerIndex,
    /// Ordered by latest -> oldest, ie the first element will be this ledger's parent.
    pub ancestors: Vec<Digest>,
    pub batch_set: Vec<Digest>,
}

impl Display for Ledger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Ledger {
    pub fn new(seq: LedgerIndex, ancestors: Vec<Digest>, batch_set: HashSet<Digest>) -> Self {
        let mut ordered_set: Vec<Digest> = batch_set.into_iter()
            .collect();
        ordered_set.sort();
        Self {
            id: Self::compute_id(seq, &ancestors, &ordered_set),
            seq,
            ancestors,
            batch_set: ordered_set,
        }
    }

    pub fn compute_id(seq: LedgerIndex, ancestors: &Vec<Digest>, batch_set: &Vec<Digest>) -> Digest {
        bincode::serialize(&(seq, ancestors, batch_set)).unwrap().as_slice().digest()
    }

    fn min_seq(&self) -> LedgerIndex {
        self.seq - std::cmp::min(self.seq, self.ancestors.len() as u32)
    }
}

impl xrpl_consensus_core::Ledger for Ledger {
    type IdType = Digest;

    fn id(&self) -> Self::IdType {
        self.id
    }

    fn seq(&self) -> LedgerIndex {
        self.seq
    }

    fn get_ancestor(&self, seq: LedgerIndex) -> Self::IdType {
        if seq >= self.min_seq() && seq <= self.seq() {
            if seq == self.seq() {
                return self.id();
            }

            if seq == 0 {
                return Ledger::make_genesis().id
            }

            let diff = self.seq() - seq;
            // info!("Ledger {:?} ancestors: {:?}. Requested seq = {:?}", (self.id, self.seq), self.ancestors, seq);
            let digest = self.ancestors.get(self.ancestors.len() - diff as usize - 1);
            if digest.is_none() {
                error!(
                    "Ledger about to panic. id = {:?}, seq {:?}, ancestors: {:?}. Requested ancestor seq = {:?}",
                    self.id,
                    self.seq,
                    self.ancestors,
                    seq
                );
            }
            return *digest.unwrap();
        }

        Digest::default()
    }

    fn make_genesis() -> Self {
        Self {
            id: Default::default(),
            seq: 0,
            ancestors: vec![],
            batch_set: Default::default(),
        }
    }

    fn mismatch(&self, other: &Self) -> LedgerIndex {
        let lower = std::cmp::max(self.min_seq(), other.min_seq());
        let upper = std::cmp::min(self.seq(), other.seq());

        let mut curr = upper;
        while curr != 0 && self.get_ancestor(curr) != other.get_ancestor(curr) && curr >= lower {
            curr -= 1;
        }

        if curr < lower {
            1
        } else {
            curr + 1
        }
    }
}