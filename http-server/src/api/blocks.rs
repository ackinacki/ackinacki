use std::sync::Arc;

use salvo::prelude::*;

pub type GetBlockFn = Arc<dyn Fn(Vec<u8>) -> anyhow::Result<Vec<u8>> + Send + Sync>;

pub struct BlocksBlockHandler {
    pub get_block_by_id: GetBlockFn,
}

impl BlocksBlockHandler {
    pub fn new(get_block_by_id: GetBlockFn) -> Self {
        Self { get_block_by_id }
    }
}

#[async_trait]
impl Handler for BlocksBlockHandler {
    async fn handle(
        &self,
        req: &mut Request,
        _depot: &mut Depot,
        res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        let _v: Vec<u8> = Vec::new();
        let id = req.param::<Vec<u8>>("id").unwrap();

        tracing::info!("id: {:?}", id);

        let _ = (self.get_block_by_id)(id.clone()).unwrap();
        // res.write_body(v);
        let _ = res.write_body(id);
    }
}
