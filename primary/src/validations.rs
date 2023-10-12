use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use xrpl_consensus_core::LedgerIndex;
use crypto::{Digest, PublicKey, Signature, SignatureService};
use crate::Ledger;
use xrpl_consensus_core::Ledger as LedgerTrait;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct Validation {
    seq: LedgerIndex,
    pub ledger_id: Digest,
    sign_time: SystemTime,
    seen_time: SystemTime,
    pub node_id: PublicKey,
    signing_pub_key: PublicKey,
    full: bool,
    trusted: bool,
    cookie: u64,
}

impl Validation {
    pub fn new(
        seq: LedgerIndex,
        ledger_id: Digest,
        sign_time: SystemTime,
        seen_time: SystemTime,
        node_id: PublicKey,
        signing_pub_key: PublicKey,
        full: bool,
        trusted: bool,
        cookie: u64,
    ) -> Self {
        Validation {
            seq,
            ledger_id,
            sign_time,
            seen_time,
            node_id,
            signing_pub_key,
            full,
            trusted,
            cookie,
        }
    }

    pub async fn sign(self, signature_service: &mut SignatureService) -> SignedValidation {
        let signature = signature_service.sign(bincode::serialize(&self).unwrap()).await;
        SignedValidation {
            validation: self,
            signature,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct SignedValidation {
    pub validation: Validation,
    signature: Signature,

}

impl SignedValidation {
    pub fn verify(&self) -> bool {
        self.signature.verify_msg(
            bincode::serialize(&self.validation).unwrap().as_slice(),
            &self.validation.signing_pub_key
        ).is_ok()
    }
}

impl xrpl_consensus_core::Validation for SignedValidation {
    type LedgerIdType = <Ledger as LedgerTrait>::IdType;

    fn seq(&self) -> LedgerIndex {
        self.validation.seq
    }

    fn ledger_id(&self) -> Self::LedgerIdType {
        self.validation.ledger_id
    }

    fn sign_time(&self) -> SystemTime {
        self.validation.sign_time
    }

    fn seen_time(&self) -> SystemTime {
        self.validation.seen_time
    }

    fn cookie(&self) -> u64 {
        self.validation.cookie
    }

    fn trusted(&self) -> bool {
        self.validation.trusted
    }

    fn full(&self) -> bool {
        self.validation.full
    }

    fn load_fee(&self) -> Option<u32> {
        None
    }
}