// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
mod state_sync_service_trait;
pub use state_sync_service_trait::StateSyncService;

mod external_fileshares_based;
pub use external_fileshares_based::ExternalFileSharesBased;

mod stub;
pub use stub::StateSyncServiceStub;

mod file_saving_service;
pub use file_saving_service::FileSavingService;

pub const GOSSIP_API_ADVERTISE_ADDR_KEY: &str = "api_advertise_addr";
