use std::collections::HashSet;
use std::fmt::{Display, Formatter};
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
    batch_set: HashSet<Digest>,
}

impl Display for Ledger {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Ledger {
    pub fn new(seq: LedgerIndex, ancestors: Vec<Digest>, batch_set: HashSet<Digest>) -> Self {
        Self {
            id: Self::compute_id(seq, &ancestors, &batch_set),
            seq,
            ancestors,
            batch_set,
        }
    }

    pub fn compute_id(seq: LedgerIndex, ancestors: &Vec<Digest>, batch_set: &HashSet<Digest>) -> Digest {
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

            let diff = self.seq() - seq;
            return *self.ancestors.get(self.ancestors.len() - diff as usize).unwrap();
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