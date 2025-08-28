use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

pub trait Subscriber {
    fn handle_start_thread(
        &mut self,
        parent_split_block: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
        threads_table: Option<ThreadsTable>,
    );
    fn handle_stop_thread(
        &mut self,
        last_thread_block: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
    );
}

macro_rules! tuple_subs {
    ($($I:tt $T: tt),+) => {
        impl<$($T,)+> Subscriber for ($(&mut $T,)+)
        where
            $($T: Subscriber,)+
        {
            fn handle_start_thread(
                &mut self,
                parent_split_block: &BlockIdentifier,
                thread_identifier: &ThreadIdentifier,
                threads_table: Option<ThreadsTable>,
            ) {
                $(
                self.$I.handle_start_thread(
                    parent_split_block,
                    thread_identifier,
                    threads_table.clone(),
                );
                )+
            }
            fn handle_stop_thread(
                &mut self,
                last_thread_block: &BlockIdentifier,
                thread_identifier: &ThreadIdentifier,
            ) {
                $(
                self.$I.handle_stop_thread(
                    last_thread_block,
                    thread_identifier,
                );
                )+
            }

        }
    }
}

tuple_subs!(0 A);
tuple_subs!(0 A, 1 B);
tuple_subs!(0 A, 1 B, 2 C);
tuple_subs!(0 A, 1 B, 2 C, 3 D);
tuple_subs!(0 A, 1 B, 2 C, 3 D, 4 E);
