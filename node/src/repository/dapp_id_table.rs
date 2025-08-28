use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use crate::types::AccountAddress;
use crate::types::BlockEndLT;
use crate::types::DAppIdentifier;

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct DAppIdTable {
    inner: Arc<BTreeMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>>,
}

#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DAppIdTableChangeSet {
    inner: BTreeMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
}

impl DAppIdTableChangeSet {
    pub fn insert(
        &mut self,
        address: AccountAddress,
        dapp_id: Option<DAppIdentifier>,
        block_lt: BlockEndLT,
    ) {
        self.inner
            .entry(address)
            .and_modify(|cur| {
                if block_lt > cur.1 {
                    *cur = (dapp_id.clone(), block_lt.clone());
                }
            })
            .or_insert((dapp_id, block_lt));
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&AccountAddress, &(Option<DAppIdentifier>, BlockEndLT))> {
        self.inner.iter()
    }

    pub fn get_value(
        &self,
        address: &AccountAddress,
    ) -> Option<&(Option<DAppIdentifier>, BlockEndLT)> {
        self.inner.get(address)
    }
}

impl DAppIdTable {
    pub fn get(&self, address: &AccountAddress) -> Option<&(Option<DAppIdentifier>, BlockEndLT)> {
        self.inner.get(address)
    }

    pub fn apply_change_set(self, change_set: &DAppIdTableChangeSet) -> Self {
        if !change_set.is_empty() {
            let mut table = Arc::unwrap_or_clone(self.inner);
            // TODO: need to think of how to merge dapp id tables, because accounts can be deleted and created in both threads
            // Possible solution is to store tuple (Option<Value>, timestamp) as a value and compare timestamps on merge.
            for (account_address, (dapp_id, lt)) in &change_set.inner {
                table
                    .entry(account_address.clone())
                    .and_modify(|data| {
                        if data.1 < *lt {
                            *data = (dapp_id.clone(), lt.clone())
                        }
                    })
                    .or_insert_with(|| (dapp_id.clone(), lt.clone()));
            }
            DAppIdTable { inner: Arc::new(table) }
        } else {
            self
        }
    }
}
