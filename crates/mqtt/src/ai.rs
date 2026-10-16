//! AI detection object classes reported by cameras and doorbells.

/// One class of object a camera's AI detector reports. Drives the
/// per-object status topic and the Home Assistant binary sensor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AiObject {
	Person,
	Vehicle,
	Animal,
	Face,
	Package,
}

impl AiObject {
	/// Every object class, in discovery emission order.
	pub const ALL: [AiObject; 5] = [
		AiObject::Person,
		AiObject::Vehicle,
		AiObject::Animal,
		AiObject::Face,
		AiObject::Package,
	];

	/// Lower-case name used in topics and unique-id suffixes.
	pub fn topic_name(self) -> &'static str {
		match self {
			AiObject::Person => "person",
			AiObject::Vehicle => "vehicle",
			AiObject::Animal => "animal",
			AiObject::Face => "face",
			AiObject::Package => "package",
		}
	}

	/// Entity display name in Home Assistant.
	pub fn display_name(self) -> &'static str {
		match self {
			AiObject::Person => "Person",
			AiObject::Vehicle => "Vehicle",
			AiObject::Animal => "Animal",
			AiObject::Face => "Face",
			AiObject::Package => "Package",
		}
	}

	/// Material Design icon of the entity.
	pub fn icon(self) -> &'static str {
		match self {
			AiObject::Person => "mdi:account",
			AiObject::Vehicle => "mdi:car",
			AiObject::Animal => "mdi:paw",
			AiObject::Face => "mdi:face-recognition",
			AiObject::Package => "mdi:package-variant-closed",
		}
	}

	/// Home Assistant `binary_sensor` device class.
	pub fn device_class(self) -> &'static str {
		match self {
			AiObject::Package => "presence",
			_ => "motion",
		}
	}

	/// Classes only doorbell-type hardware reports.
	pub fn is_doorbell_only(self) -> bool {
		matches!(self, AiObject::Face | AiObject::Package)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn topic_names_match_spec() {
		let names: Vec<_> = AiObject::ALL.iter().map(|o| o.topic_name()).collect();
		assert_eq!(names, ["person", "vehicle", "animal", "face", "package"]);
	}

	#[test]
	fn only_face_and_package_are_doorbell_only() {
		let only: Vec<_> = AiObject::ALL
			.into_iter()
			.filter(|o| o.is_doorbell_only())
			.collect();
		assert_eq!(only, [AiObject::Face, AiObject::Package]);
	}
}
