#![cfg(test)]
mod fork_tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::str::FromStr;

    use crate::node::block_state::repository::BlockState;
    use crate::node::services::fork_resolution::service::find_fork_winner;
    use crate::node::services::fork_resolution::service::Fork;
    use crate::node::SignerIndex;
    use crate::types::BlockIdentifier;

    fn prepare_fork(total_number_of_bks: usize, number_of_signers: Vec<SignerIndex>) -> Fork {
        let parent_state = BlockState::test();
        let mut signer_index = 0 as SignerIndex;
        let mut fork_attestation_signers = HashMap::new();
        for (i, candidate) in number_of_signers.iter().enumerate() {
            let block_id = BlockIdentifier::from_str(&format!("{:064X}", i + 1)).unwrap();
            let signers = HashSet::from_iter(signer_index..signer_index + *candidate);
            signer_index += *candidate;
            fork_attestation_signers.insert(block_id, signers);
        }
        Fork::builder()
            .parent_block_state(parent_state)
            .fork_blocks(vec![])
            .fork_attestation_signers(fork_attestation_signers)
            .fork_attestations(HashMap::new())
            .total_number_of_bks(Some(total_number_of_bks))
            .build()
    }

    #[test]
    fn test_fork_winner() {
        // Single participant all participants has voted
        let fork = prepare_fork(20, vec![20]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 1)).ok());

        // Single participant 2/3 of participants has voted
        let fork = prepare_fork(20, vec![14]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 1)).ok());

        // Two candidates, all participants has voted, one candidate with 2/3 of votes
        let fork = prepare_fork(20, vec![6, 14]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 2)).ok());

        // Two candidates, more than 2/3 of all participants has voted, one candidate is a leader
        let fork = prepare_fork(20, vec![6, 11]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 2)).ok());

        // Two candidates, all participants has voted, equal votes split
        let fork = prepare_fork(20, vec![10, 10]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 2)).ok());

        // More candidates, all participants has voted
        let fork = prepare_fork(20, vec![4, 3, 3, 3, 4, 3]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, BlockIdentifier::from_str(&format!("{:064X}", 5)).ok());

        // Single participant, not enough votes
        let fork = prepare_fork(20, vec![13]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, None);

        // Two participants, not enough votes
        let fork = prepare_fork(20, vec![6, 6]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, None);

        // Two participants, not enough votes
        let fork = prepare_fork(20, vec![9, 9]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, None);

        // Several participants, not enough votes
        let fork = prepare_fork(20, vec![3, 3, 3, 3, 3, 3]);
        let res = find_fork_winner(&fork);
        assert_eq!(res, None);
    }
}
