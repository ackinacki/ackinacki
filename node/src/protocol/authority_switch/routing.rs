use node_types::ThreadIdentifier;

use super::network_message::AuthoritySwitch;
use crate::bls::envelope::BLSSignedEnvelope;

// This extends message dispatcher part of the node.
//
pub fn route(message: &AuthoritySwitch) -> ThreadIdentifier {
    match message {
        AuthoritySwitch::Request(e) => *e.lock().data().height().thread_identifier(),
        AuthoritySwitch::Reject(e) => *e.thread_identifier(),
        AuthoritySwitch::RejectTooOld(e) => *e,
        AuthoritySwitch::Switched(e) => *e.data().block_height().thread_identifier(),
        AuthoritySwitch::Failed(e) => *e.data().block_height().thread_identifier(),
    }
}
