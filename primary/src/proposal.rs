use std::collections::HashSet;
//use std::convert::TryInto;
use serde::{Deserialize, Serialize};
use xrpl_consensus_core::LedgerIndex;

use config::WorkerId;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct ConsensusRound(u8);

impl From<u8> for ConsensusRound {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl ConsensusRound {
    pub fn next(mut self) -> Self {
        self.0 += 1;
        self
    }

    pub fn reset(mut self) -> Self {
        self.0 = 0;
        self
    }

    pub fn threshold(&self) -> f32 {
        match self.0 {
            0 => 0.5,
            1 => 0.65,
            2 => 0.70,
            _ => 0.95
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Proposal {
    pub round: ConsensusRound,
    pub parent_id: Digest,
    ledger_index: LedgerIndex,
    pub batches: HashSet<(Digest, WorkerId)>,
    pub node_id: PublicKey
}

impl Proposal {
    pub fn new(
        round: ConsensusRound,
        parent_id: Digest,
        ledger_index: LedgerIndex,
        batches: HashSet<(Digest, WorkerId)>,
        node_id: PublicKey,
    ) -> Self {
        Proposal {
            round,
            parent_id,
            ledger_index,
            batches,
            node_id,
        }
    }

    pub async fn sign(self, sig_service: &mut SignatureService) -> SignedProposal {
        let signature = sig_service.sign(bincode::serialize(&self).unwrap()).await;
        SignedProposal {
            proposal: self,
            signature,
        }
    }

    pub fn compute_id(self) -> Digest {
        bincode::serialize(&(self.round, self.parent_id, self.ledger_index, self.batches, self.node_id)).unwrap().as_slice().digest()
    }
}

// impl Hash for Proposal {
//     fn digest(&self) -> Digest {
//         let mut hasher = Sha512::new();
//         hasher.update(&self.author);
//         hasher.update(self.round.to_le_bytes());
//         for (x, y) in &self.payload {
//             hasher.update(x);
//             hasher.update(y.to_le_bytes());
//         }
//         for x in &self.parents {
//             hasher.update(x);
//         }
//         Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
//     }
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedProposal {
    pub proposal: Proposal,
    signature: Signature
}

impl SignedProposal {

    pub fn verify(&self) -> bool {
        self.signature.verify_msg(
            bincode::serialize(&self.proposal).unwrap().as_slice(),
            &self.proposal.node_id
        ).is_ok()
    }

    pub fn node_id(&self) -> PublicKey {
        self.proposal.node_id
    }
}