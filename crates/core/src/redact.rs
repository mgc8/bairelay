//! Log-safe renderings of identifiers that info-level logs carry.
//!
//! Info / warn logs routinely end up pasted into bug reports, so a camera
//! UID there shows only its tail — enough to tell cameras apart and match
//! a heartbeat to a config entry. Debug logs keep the full value.

use std::fmt;

/// Characters of a UID shown at info / warn level.
pub const UID_VISIBLE_TAIL: usize = 6;

/// `Display` wrapper rendering a UID as `*` padding plus its last
/// [`UID_VISIBLE_TAIL`] characters, preserving the original length so
/// short-form (16-char) and firmware-suffixed long-form UIDs stay
/// distinguishable. UIDs no longer than the tail are fully masked.
///
/// ```
/// use bairelay_neolink_core::redact::UidTail;
/// assert_eq!(UidTail("9527000ABCDEFGHI").to_string(), "**********DEFGHI");
/// ```
pub struct UidTail<'a>(pub &'a str);

impl fmt::Display for UidTail<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// Char-based, not byte-based: UIDs parsed off the wire are
		// untrusted and may carry multi-byte characters.
		let len = self.0.chars().count();
		let shown = if len > UID_VISIBLE_TAIL {
			UID_VISIBLE_TAIL
		} else {
			0
		};
		for _ in 0..len - shown {
			f.write_str("*")?;
		}
		for c in self.0.chars().skip(len - shown) {
			fmt::Write::write_char(f, c)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn short_form_uid_shows_tail_only() {
		assert_eq!(UidTail("9527000ABCDEFGHI").to_string(), "**********DEFGHI");
	}

	#[test]
	fn long_form_uid_keeps_length() {
		let out = UidTail("9527000ABCDEFGHI0123").to_string();
		assert_eq!(out, "**************HI0123");
		assert_eq!(out.len(), 20);
	}

	#[test]
	fn uid_at_or_below_tail_is_fully_masked() {
		assert_eq!(UidTail("ABCDEF").to_string(), "******");
		assert_eq!(UidTail("AB").to_string(), "**");
		assert_eq!(UidTail("").to_string(), "");
	}

	#[test]
	fn multibyte_input_is_char_safe() {
		assert_eq!(UidTail("ééééééééé").to_string(), "***éééééé");
	}
}
