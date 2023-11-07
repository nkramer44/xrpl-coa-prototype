use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use xrpl_consensus_core::{Ledger as LedgerTrait, NetClock, WallNetClock};
use xrpl_consensus_validations::Adaptor;

use crypto::PublicKey;
use primary::{Ledger, SignedValidation};

pub struct ValidationsAdaptor {
    clock: Arc<RwLock<WallNetClock>>,
    // Assumption: When core gets a validation from the network, it will not pass it to
    // consensus until it has the ledger synced locally in LedgerMaster.
    // So, this struct does not need to tell LedgerMaster to acquire anything. Instead,
    // it should keep a local cache of ledgers that it can pass to Validations when it asks
    // to acquire. This cache will be updated when a PrimaryConsensusMessage::SyncedLedger(Ledger)
    // message gets sent from Primary. In theory, this cache should have 100% hit rate, as long
    // as the primary sends the SyncedLedger message before it sends the Validation.
    ledger_cache: HashMap<<Self as Adaptor>::LedgerIdType, Ledger>
}

impl ValidationsAdaptor {
    pub fn new(
        clock: Arc<RwLock<WallNetClock>>
    ) -> Self {
        let mut ledger_cache: HashMap<<Self as Adaptor>::LedgerIdType, Ledger> = Default::default();
        let genesis = Ledger::make_genesis();
        ledger_cache.insert(genesis.id, genesis);
        Self { clock, ledger_cache }
    }

    pub fn add_ledger(&mut self, ledger: Ledger) {
        self.ledger_cache.insert(ledger.id(), ledger);
    }
}

#[async_trait]
impl Adaptor for ValidationsAdaptor {
    type ValidationType = SignedValidation;
    type LedgerType = Ledger;
    type LedgerIdType = <Ledger as LedgerTrait>::IdType;
    type NodeIdType = PublicKey;
    type NodeKeyType = PublicKey;
    type ClockType = WallNetClock;

    fn now(&self) -> SystemTime {
        self.clock.read().unwrap().now()
    }

    async fn acquire(&mut self, ledger_id: &Self::LedgerIdType) -> Option<Self::LedgerType> {
        // Note: This will panic if the ledger isn't in the ledger_cache because we assume that
        // the ledger will be loaded into the cache before we process any validations. If it
        // is not there, it means there is a bug in our software.
        let ledger = self.ledger_cache.get(ledger_id)
            .expect(format!("Adaptor did not have Ledger {:?} in cache.", ledger_id).as_str());
        Some(ledger.clone())
    }
}