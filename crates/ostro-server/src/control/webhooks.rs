//! Webhook controller: `ostro_api::phase8::WebhookApi` (meta `Webhook` CRUD, CAS on the
//! record version; secrets referenced by name only) and `HookIngress` (`POST
//! /hooks/{name}`: verify the raw body with ostro-triggers using a sealed secret from
//! [`super::ControlCtx::secrets`] and the timestamp from [`super::ControlCtx::clock`],
//! then start / signal / complete-token, answering 2xx after apply). **Owned by 08-15**;
//! a stub until then (every method answers `Unavailable`, "not yet implemented (08-15)").

use ostro_api::ApiError;
use ostro_api::phase8::{HookIngress, HookRequest, HookResponse, WebhookApi, WebhookDto};

use super::{ControlCtx, not_yet};

/// The plan that implements this controller.
const OWNER: &str = "08-15";

/// `WebhookApi` + `HookIngress` for the server.
#[derive(Debug)]
pub struct WebhookController {
    ctx: ControlCtx,
}

impl WebhookController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }
}

#[async_trait::async_trait]
impl WebhookApi for WebhookController {
    async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn get_webhook(&self, _name: String) -> Result<WebhookDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn create_webhook(&self, _w: WebhookDto) -> Result<WebhookDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn update_webhook(&self, _w: WebhookDto) -> Result<WebhookDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn delete_webhook(
        &self,
        _name: String,
        _version_tag: Option<u64>,
    ) -> Result<(), ApiError> {
        Err(not_yet(OWNER))
    }
}

#[async_trait::async_trait]
impl HookIngress for WebhookController {
    async fn deliver(&self, _req: HookRequest) -> Result<HookResponse, ApiError> {
        Err(not_yet(OWNER))
    }
}
