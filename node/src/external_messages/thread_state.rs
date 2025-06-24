use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use super::progress::Progress;
use super::queue::ExternalMessagesQueue;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::WrappedMessage;
use crate::repository::repository_impl::load_from_file;
use crate::repository::repository_impl::save_to_file;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=anyhow::Result<ExternalMessagesThreadState>),
    builder_method(vis="pub"),
    builder_type(vis="pub", name=ExternalMessagesThreadStateBuilder),
    field_defaults(setter(prefix="with_")),
)]
struct ExternalMessagesThreadStateConfig {
    block_progress_data_dir: PathBuf,
    report_metrics: Option<BlockProductionMetrics>,
    thread_id: ThreadIdentifier,
    cache_size: usize,
}

impl std::convert::From<ExternalMessagesThreadStateConfig>
    for anyhow::Result<ExternalMessagesThreadState>
{
    fn from(
        config: ExternalMessagesThreadStateConfig,
    ) -> anyhow::Result<ExternalMessagesThreadState> {
        let ExternalMessagesThreadStateConfig {
            block_progress_data_dir,
            report_metrics,
            thread_id,
            cache_size,
        } = config;
        let queue: ExternalMessagesQueue = ExternalMessagesQueue::empty();
        Ok(ExternalMessagesThreadState {
            block_progress_data_dir,
            queue: Arc::new(Mutex::new(queue)),
            block_progress_access_mutex: Arc::new(Mutex::new(())),
            report_metrics,
            thread_id,
            cache_size,
        })
    }
}

// Note:
// It is important to keep external messages and their progress in sync.
// If one is stored in a durable storage the other one must be stored aswell.
// This is the main reason why progress is not stored in BlockState directly,
// and rather stored together with the external messages.
// The other side effect is that the progress for external messages
// is different for each thread while block may exist in several threads
// simultaneously (thread starting block).
#[derive(Clone)]
pub struct ExternalMessagesThreadState {
    block_progress_access_mutex: Arc<Mutex<()>>,
    block_progress_data_dir: PathBuf,

    queue: Arc<Mutex<ExternalMessagesQueue>>,
    report_metrics: Option<BlockProductionMetrics>,
    // For reporting only.
    thread_id: ThreadIdentifier,
    cache_size: usize,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn set_progress(
        &self,
        block_identifier: &BlockIdentifier,
        progress: Progress,
    ) -> anyhow::Result<()> {
        tracing::trace!("set_progress {:?}", block_identifier);
        self.block_progress_access_mutex.guarded(|_| {
            let file_path = self.progress_file_path(block_identifier);
            if let Some(saved_progress) = load_from_file::<Progress>(&file_path)? {
                assert!(progress == saved_progress);
                return Ok(());
            }
            save_to_file(&file_path, &progress, true)
        })
    }

    pub fn get_progress(
        &self,
        block_identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Progress>> {
        self.block_progress_access_mutex.guarded(|_| {
            let file_path = self.progress_file_path(block_identifier);
            load_from_file::<Progress>(&file_path)
        })
    }

    pub fn set_progress_to_last_known(
        &self,
        block_identifier: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("set_progress_to_last_known {:?}", block_identifier);
        self.queue.guarded(|e| {
            let offset = e.last_index();
            let progress = Progress::zero().next(*offset as usize);
            self.set_progress(block_identifier, progress)
        })
    }

    pub fn push_external_messages(&self, messages: &[WrappedMessage]) -> anyhow::Result<()> {
        tracing::trace!("add_external_messages {:?}", messages.len());
        let report_len = self.queue.guarded_mut(|e| -> anyhow::Result<usize> {
            let rest = self.cache_size.saturating_sub(e.messages().len()).min(messages.len());
            if rest > 0 {
                e.push_external_messages(messages.split_at(rest).0);
                tracing::trace!("cur cache_size len {:?}", e.messages().len());
            }
            Ok(e.messages().len())
        })?;
        self.report_metrics
            .as_ref()
            .inspect(|e| e.report_ext_msg_queue_size(report_len, &self.thread_id));
        Ok(())
    }

    pub fn erase_outdated(&self, current_progress: &Progress) -> anyhow::Result<()> {
        let report_len = self.queue.guarded_mut(|e| -> anyhow::Result<usize> {
            e.erase_till(current_progress);
            let report = e.messages().len();
            Ok(report)
        })?;
        self.report_metrics
            .as_ref()
            .inspect(|e| e.report_ext_msg_queue_size(report_len, &self.thread_id));
        Ok(())
    }

    pub fn get_remaining_external_messages(
        &self,
        block_identifier: &BlockIdentifier,
    ) -> anyhow::Result<Vec<WrappedMessage>> {
        let Some(progress) = self.get_progress(block_identifier)? else {
            anyhow::bail!("Block progress was not stored. block id: {}", block_identifier);
        };
        self.queue.guarded(|e| Ok(e.clone_tail(&progress)))
    }

    fn progress_file_path(&self, block_identifier: &BlockIdentifier) -> PathBuf {
        self.block_progress_data_dir.join(format!("{:x}", block_identifier))
    }
}
