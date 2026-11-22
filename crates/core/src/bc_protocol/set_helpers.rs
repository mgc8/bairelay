//! Shared "wait for SET reply with the don't-bother-replying quirk"
//! helper. Several SET-style commands across the protocol surface
//! ([`set_floodlight_manual`], [`set_ledstate`], [`set_pirstate`],
//! [`set_services`], [`set_users`]) share an identical wait shape:
//! send the request, wait up to 500 ms for a reply, treat
//! response_code 200 as success, treat any non-200 as
//! [`CameraServiceUnavailable`], and treat the absence of a reply
//! within the window as success — some firmwares silently accept SET
//! commands without bothering to acknowledge.
//!
//! [`set_floodlight_manual`]: super::BcCamera::set_floodlight_manual
//! [`set_ledstate`]: super::BcCamera::set_ledstate
//! [`set_pirstate`]: super::BcCamera::set_pirstate
//! [`set_services`]: super::BcCamera::set_services
//! [`set_users`]: super::BcCamera::set_users
//! [`CameraServiceUnavailable`]: crate::Error::CameraServiceUnavailable

use std::time::Duration;

use super::connection::BcSubscription;
use super::Error;
use crate::Result;

/// 500 ms wait for the camera's reply before treating absence as
/// success. Empirically, some firmwares (observed across multiple
/// SET commands on Argus and friends) accept the request silently;
/// others reply within tens of ms. The 500 ms cushion covers slow
/// replies on a busy session without holding the caller noticeably.
pub(super) const SET_QUIRK_TIMEOUT: Duration = Duration::from_millis(500);

/// Wait for a SET-style reply with the no-reply-on-success quirk.
///
/// Returns `Ok(())` when the camera replies with `response_code = 200`
/// or when no reply lands within `quirk` (some firmwares stay silent
/// on success). Returns [`Error::CameraServiceUnavailable`] when the
/// camera replies with a non-200 status, surfacing the rejected
/// `msg_id` and `code` for the caller to log.
///
/// Errors from the underlying subscription (channel closed, connection
/// dropped) propagate as [`Error::DroppedSubscriber`] / [`Error::Other`]
/// via the `?` operator on `sub.recv()`.
pub(super) async fn await_set_reply_with_quirk(
	sub: &mut BcSubscription<'_>,
	quirk: Duration,
) -> Result<()> {
	let Ok(reply) = tokio::time::timeout(quirk, sub.recv()).await else {
		// Quirk: camera didn't reply within the window. Treat as
		// success — observed firmwares silently accept SETs.
		return Ok(());
	};
	let msg = reply?;
	if msg.meta.response_code == 200 {
		Ok(())
	} else {
		Err(Error::CameraServiceUnavailable {
			id: msg.meta.msg_id,
			code: msg.meta.response_code,
		})
	}
}
