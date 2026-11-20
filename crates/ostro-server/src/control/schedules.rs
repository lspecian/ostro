//! Cron schedule controller: `ostro_api::phase8::ScheduleApi` over meta `Schedule`
//! records (CAS on the record version) and the partitions (`ScheduleFire{kind: Manual}`
//! for trigger-now). **Owned by 08-14**; a stub until then (every method answers
//! `Unavailable`, "not yet implemented (08-14)").
//!
//! The overlap / catch-up wire values map to the meta u8 constants through
//! [`overlap_to_meta`] / [`catchup_to_meta`] and back (05-07 numbering, cross-checked
//! against `ostro_meta::records` and `ostro_triggers::cron` in the tests).

use ostro_api::ApiError;
use ostro_api::phase8::{CatchupPolicyDto, OverlapPolicyDto, ScheduleApi, ScheduleDto};

use super::{ControlCtx, not_yet};

/// The plan that implements this controller.
const OWNER: &str = "08-14";

/// `ScheduleApi` for the server.
#[derive(Debug)]
pub struct ScheduleController {
    ctx: ControlCtx,
}

impl ScheduleController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }
}

/// Wire overlap policy -> meta `OVERLAP_*`.
pub fn overlap_to_meta(p: OverlapPolicyDto) -> u8 {
    p.as_u8()
}

/// Meta `OVERLAP_*` -> wire overlap policy (`Internal` on an out-of-range record).
pub fn overlap_from_meta(v: u8) -> Result<OverlapPolicyDto, ApiError> {
    OverlapPolicyDto::from_u8(v)
        .ok_or_else(|| ApiError::internal(format!("stored schedule has overlap {v}")))
}

/// Wire catch-up policy -> meta `CATCHUP_*`.
pub fn catchup_to_meta(p: CatchupPolicyDto) -> u8 {
    p.as_u8()
}

/// Meta `CATCHUP_*` -> wire catch-up policy (`Internal` on an out-of-range record).
pub fn catchup_from_meta(v: u8) -> Result<CatchupPolicyDto, ApiError> {
    CatchupPolicyDto::from_u8(v)
        .ok_or_else(|| ApiError::internal(format!("stored schedule has catchup {v}")))
}

#[async_trait::async_trait]
impl ScheduleApi for ScheduleController {
    async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn get_schedule(&self, _name: String) -> Result<ScheduleDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn create_schedule(&self, _s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn update_schedule(&self, _s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn delete_schedule(
        &self,
        _name: String,
        _version_tag: Option<u64>,
    ) -> Result<(), ApiError> {
        Err(not_yet(OWNER))
    }

    async fn pause_schedule(&self, _name: String) -> Result<ScheduleDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn resume_schedule(&self, _name: String) -> Result<ScheduleDto, ApiError> {
        Err(not_yet(OWNER))
    }

    async fn trigger_schedule(
        &self,
        _name: String,
        _request_id: Option<String>,
    ) -> Result<String, ApiError> {
        Err(not_yet(OWNER))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_meta::records::{
        CATCHUP_ALL, CATCHUP_ONE, CATCHUP_SKIP, OVERLAP_ALLOW_ALL, OVERLAP_BUFFER_ONE,
        OVERLAP_CANCEL_OTHER, OVERLAP_SKIP, OVERLAP_TERMINATE_OTHER,
    };
    use ostro_triggers::cron::{CatchupPolicy, OverlapPolicy};

    #[test]
    fn overlap_mapping_matches_meta_and_triggers() {
        for (dto, meta, trig) in [
            (OverlapPolicyDto::Skip, OVERLAP_SKIP, OverlapPolicy::Skip),
            (
                OverlapPolicyDto::BufferOne,
                OVERLAP_BUFFER_ONE,
                OverlapPolicy::BufferOne,
            ),
            (
                OverlapPolicyDto::AllowAll,
                OVERLAP_ALLOW_ALL,
                OverlapPolicy::AllowAll,
            ),
            (
                OverlapPolicyDto::CancelOther,
                OVERLAP_CANCEL_OTHER,
                OverlapPolicy::CancelOther,
            ),
            (
                OverlapPolicyDto::TerminateOther,
                OVERLAP_TERMINATE_OTHER,
                OverlapPolicy::TerminateOther,
            ),
        ] {
            assert_eq!(overlap_to_meta(dto), meta);
            assert_eq!(overlap_from_meta(meta).expect("known"), dto);
            assert_eq!(OverlapPolicy::try_from(meta).expect("triggers"), trig);
        }
        assert!(overlap_from_meta(5).is_err());
    }

    #[test]
    fn catchup_mapping_matches_meta_and_triggers() {
        for (dto, meta, trig) in [
            (CatchupPolicyDto::Skip, CATCHUP_SKIP, CatchupPolicy::Skip),
            (CatchupPolicyDto::One, CATCHUP_ONE, CatchupPolicy::One),
            (CatchupPolicyDto::All, CATCHUP_ALL, CatchupPolicy::All),
        ] {
            assert_eq!(catchup_to_meta(dto), meta);
            assert_eq!(catchup_from_meta(meta).expect("known"), dto);
            assert_eq!(CatchupPolicy::try_from(meta).expect("triggers"), trig);
        }
        assert!(catchup_from_meta(3).is_err());
    }
}
