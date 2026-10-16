//! Wire-format round-trip tests for every `bc::xml` struct.
//!
//!
//!
//! Strategy: wrap each inner struct in its real `BcXml` carrier
//! (that's how the protocol uses them on the wire) and round-trip
//! the whole `BcXml` via the real `try_parse` + `serialize` methods.
//! This exercises the exact serde attributes — renames, `@attr`
//! handling, `skip_serializing_if`, `#[serde(default)]` — the way
//! the runtime does.
//!
//! Each struct gets:
//! 1. A round-trip test (parse XML → serialize → re-parse → equality).
//! 2. For structs with `#[serde(default)]` fields: a drop-field test
//!    that removes each optional element and asserts the struct still
//!    parses (default value fills the gap).
//!
//! Fixtures are hand-constructed minimal XML payloads modelled after
//! real Argus responses and the Neolink project's captures.

use super::xml::*;
use indoc::indoc;

/// Round-trip a `BcXml` XML blob: parse → serialize → re-parse,
/// asserting both parses agree. Returns the parsed value so callers
/// can make per-field assertions.
fn assert_xml_roundtrip_via_bcxml(xml: &str) -> BcXml {
	let parsed = BcXml::try_parse(xml.as_bytes())
		.unwrap_or_else(|e| panic!("parse failed: {e}\nXML:\n{xml}"));
	let re = parsed
		.serialize(vec![])
		.unwrap_or_else(|e| panic!("serialize failed: {e}"));
	let reparsed = BcXml::try_parse(re.as_slice()).unwrap_or_else(|e| {
		panic!(
			"re-parse failed: {e}\nSerialized:\n{:?}",
			std::str::from_utf8(&re)
		)
	});
	let parsed2 = BcXml::try_parse(xml.as_bytes()).expect("reparse original");
	assert_eq!(parsed2, reparsed, "round-trip mismatch");
	parsed
}

// ---------------------------------------------------------------------------
// Task 22 — pilot round-trips
// ---------------------------------------------------------------------------

#[test]
fn xml_preset_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<PtzPreset version="1.1">
		<channelId>0</channelId>
		<presetList>
		<preset>
		<id>1</id>
		<name>Home</name>
		<command>toPos</command>
		</preset>
		<preset>
		<id>2</id>
		<name>Front Door</name>
		<command>setPos</command>
		</preset>
		</presetList>
		</PtzPreset>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let pp = parsed.ptz_preset.as_ref().expect("ptz_preset present");
	assert_eq!(pp.channel_id, 0);
	assert_eq!(pp.preset_list.preset.len(), 2);
	assert_eq!(pp.preset_list.preset[0].id, 1);
	assert_eq!(pp.preset_list.preset[0].name.as_deref(), Some("Home"));
	assert_eq!(pp.preset_list.preset[0].command, "toPos");
}

#[test]
fn xml_preset_tolerates_missing_command_field() {
	// Newer Argus firmware (v3.0.0.5649_25111355) omits <command>; the
	// 3A fix added #[serde(default)] on Preset.command so this parses.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<PtzPreset version="1.1">
		<channelId>0</channelId>
		<presetList>
		<preset>
		<id>1</id>
		<name>Home</name>
		</preset>
		</presetList>
		</PtzPreset>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).expect("parse with missing command");
	let pp = parsed.ptz_preset.as_ref().unwrap();
	assert_eq!(pp.preset_list.preset[0].command, "");
}

#[test]
fn xml_battery_info_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<BatteryInfo>
		<channelId>0</channelId>
		<chargeStatus>charging</chargeStatus>
		<adapterStatus>solarPanel</adapterStatus>
		<voltage>4150</voltage>
		<current>-250</current>
		<temperature>25</temperature>
		<batteryPercent>87</batteryPercent>
		<lowPower>0</lowPower>
		<batteryVersion>2</batteryVersion>
		</BatteryInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let bi = parsed.battery_info.as_ref().expect("battery_info present");
	assert_eq!(bi.channel_id, 0);
	assert_eq!(bi.charge_status, "charging");
	assert_eq!(bi.adapter_status, "solarPanel");
	assert_eq!(bi.voltage, 4150);
	assert_eq!(bi.current, -250);
	assert_eq!(bi.battery_percent, 87);
}

#[test]
fn xml_battery_info_tolerates_missing_optional_fields() {
	// Any default'd field can be absent; simulate a very sparse reply.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<BatteryInfo>
		<batteryPercent>50</batteryPercent>
		</BatteryInfo>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).expect("parse sparse battery_info");
	let bi = parsed.battery_info.as_ref().unwrap();
	assert_eq!(bi.battery_percent, 50);
	assert_eq!(bi.channel_id, 0);
	assert_eq!(bi.charge_status, "");
	assert_eq!(bi.adapter_status, "");
}

#[test]
fn xml_version_info_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<VersionInfo>
		<name>Argus</name>
		<type>Argus3</type>
		<serialNumber>ABCD12345678</serialNumber>
		<buildDay>build 25111355</buildDay>
		<hardwareVersion>IPC_585SD5</hardwareVersion>
		<cfgVersion>v3.0.0.0</cfgVersion>
		<firmwareVersion>v3.0.0.5649_25111355</firmwareVersion>
		<detail>IPC_58516M110000000100000</detail>
		</VersionInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let vi = parsed.version_info.as_ref().expect("version_info present");
	assert_eq!(vi.name, "Argus");
	assert_eq!(vi.model.as_deref(), Some("Argus3"));
	assert_eq!(vi.firmwareVersion, "v3.0.0.5649_25111355");
}

#[test]
fn xml_version_info_tolerates_missing_fields() {
	// Every field is #[serde(default)] except `model` which is skip-if-None.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<VersionInfo>
		<firmwareVersion>v1.0.0.0</firmwareVersion>
		</VersionInfo>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).expect("parse sparse version_info");
	let vi = parsed.version_info.as_ref().unwrap();
	assert_eq!(vi.firmwareVersion, "v1.0.0.0");
	assert_eq!(vi.name, "");
	assert_eq!(vi.model, None);
	assert_eq!(vi.serialNumber, "");
}

#[test]
fn xml_floodlight_status_list_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<FloodlightStatusList version="1.1">
		<FloodlightStatus>
		<channel>0</channel>
		<status>1</status>
		</FloodlightStatus>
		<FloodlightStatus>
		<channel>1</channel>
		<status>0</status>
		</FloodlightStatus>
		</FloodlightStatusList>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let list = parsed
		.floodlight_status_list
		.as_ref()
		.expect("floodlight_status_list present");
	assert_eq!(list.floodlight_status_list.len(), 2);
	assert_eq!(list.floodlight_status_list[0].channel_id, 0);
	assert_eq!(list.floodlight_status_list[0].status, 1);
	assert_eq!(list.floodlight_status_list[1].status, 0);
}

#[test]
fn xml_floodlight_status_list_empty_is_valid() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<FloodlightStatusList version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).expect("parse empty status list");
	let list = parsed.floodlight_status_list.as_ref().unwrap();
	assert!(list.floodlight_status_list.is_empty());
}

#[test]
fn xml_rf_alarm_cfg_roundtrip_legacy_firmware() {
	// Legacy firmware: has rfID, sensitivity, timeBlockList, alarmHandle.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<rfAlarmCfg version="1.1">
		<rfID>0</rfID>
		<enable>1</enable>
		<sensitivity>50</sensitivity>
		<reduceFalseAlarm>1</reduceFalseAlarm>
		<timeBlockList>
		<timeBlock>
		<enable>1</enable>
		<weekDay>Monday</weekDay>
		<beginHour>0</beginHour>
		<endHour>23</endHour>
		</timeBlock>
		</timeBlockList>
		<alarmHandle>
		<item>
		<channel>0</channel>
		<handleType>email</handleType>
		</item>
		</alarmHandle>
		</rfAlarmCfg>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let cfg = parsed.rf_alarm_cfg.as_ref().expect("rf_alarm_cfg present");
	assert_eq!(cfg.enable, 1);
	assert_eq!(cfg.rf_id, Some(0));
	assert_eq!(cfg.sensitivity, Some(50));
	assert!(cfg.time_block_list.is_some());
	assert!(cfg.alarm_handle.is_some());
}

#[test]
fn xml_rf_alarm_cfg_roundtrip_new_firmware() {
	// v1.1+ firmware: rfID / sensitivity / timeBlockList / alarmHandle absent,
	// replaced by interval / maxAlarmTime / sensiValue fields.
	let xml = indoc!(
		r#"
		<?xml version="1.1" encoding="UTF-8" ?>
		<body>
		<rfAlarmCfg version="1.1">
		<enable>1</enable>
		<sensiValue>50</sensiValue>
		<interval>30</interval>
		<maxAlarmTime>120</maxAlarmTime>
		<intervalUseRange>1</intervalUseRange>
		<intervalSecMin>15</intervalSecMin>
		<intervalSecMax>300</intervalSecMax>
		</rfAlarmCfg>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let cfg = parsed.rf_alarm_cfg.as_ref().expect("rf_alarm_cfg present");
	assert_eq!(cfg.enable, 1);
	assert_eq!(cfg.rf_id, None);
	assert_eq!(cfg.sensitivity, None);
	assert!(cfg.time_block_list.is_none());
	assert_eq!(cfg.sensi_value(), Some(50));
	assert_eq!(cfg.interval, Some(30));
	assert_eq!(cfg.max_alarm_time(), Some(120));
}

// Local-only helpers so the expression above reads cleanly regardless
// of how the field is spelled — `sensiValue` vs `maxAlarmTime` etc.
trait RfAlarmCfgAccessors {
	fn sensi_value(&self) -> Option<u8>;
	fn max_alarm_time(&self) -> Option<u32>;
}
impl RfAlarmCfgAccessors for RfAlarmCfg {
	fn sensi_value(&self) -> Option<u8> {
		self.sensiValue
	}
	fn max_alarm_time(&self) -> Option<u32> {
		self.maxAlarmTime
	}
}

// ---------------------------------------------------------------------------
// Task 23 batch A — login + device metadata structs
// ---------------------------------------------------------------------------

#[test]
fn xml_encryption_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Encryption version="1.1">
		<type>md5</type>
		<nonce>9E6D1FCB9E69846D</nonce>
		</Encryption>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let enc = parsed.encryption.as_ref().unwrap();
	assert_eq!(enc.version, "1.1");
	assert_eq!(enc.type_, "md5");
	assert_eq!(enc.nonce, "9E6D1FCB9E69846D");
}

#[test]
fn xml_encryption_tolerates_missing_type_and_nonce() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Encryption version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let enc = parsed.encryption.as_ref().unwrap();
	assert_eq!(enc.type_, "");
	assert_eq!(enc.nonce, "");
}

#[test]
fn xml_encryption_roundtrip_preserves_sigv3_ecdhe_fields() {
	// New-firmware Encryption reply: the legacy <type>/<nonce> plus the
	// sigV3/ECDHE auth negotiation. Sanitised key material (not real
	// camera values).
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Encryption version="1.1">
		<type>md5</type>
		<nonce>9E6D1FCB9E69846D</nonce>
		<authTypeList>
		<authType>password</authType>
		<authType>sigV3</authType>
		</authTypeList>
		<sigVer>v3</sigVer>
		<ECDHE>
		<publicKeyAlgo>X25519</publicKeyAlgo>
		<publicKey>cVDyUYZvGuKK</publicKey>
		<publicKeySign>KO0nVOr9ux0</publicKeySign>
		<iterations>1000</iterations>
		</ECDHE>
		</Encryption>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let enc = parsed.encryption.as_ref().unwrap();
	assert_eq!(enc.version, "1.1");
	assert_eq!(enc.type_, "md5");
	assert_eq!(enc.nonce, "9E6D1FCB9E69846D");
	assert_eq!(enc.sig_ver.as_deref(), Some("v3"));
	let atl = enc.auth_type_list.as_ref().expect("authTypeList present");
	assert_eq!(
		atl.auth_type,
		vec!["password".to_string(), "sigV3".to_string()]
	);
	let ecdhe = enc.ecdhe.as_ref().expect("ECDHE present");
	assert_eq!(ecdhe.public_key_algo, "X25519");
	assert_eq!(ecdhe.public_key, "cVDyUYZvGuKK");
	assert_eq!(ecdhe.public_key_sign, "KO0nVOr9ux0");
	assert_eq!(ecdhe.iterations, 1000);
}

#[test]
fn xml_encryption_legacy_block_defaults_new_fields() {
	// Old-firmware reply: no sigV3/ECDHE. New fields default to
	// absent so the legacy login path stays selected.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Encryption version="1.1">
		<type>md5</type>
		<nonce>9E6D1FCB9E69846D</nonce>
		</Encryption>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let enc = parsed.encryption.as_ref().unwrap();
	assert!(enc.sig_ver.is_none());
	assert!(enc.ecdhe.is_none());
	assert!(enc.auth_type_list.is_none());
}

#[test]
fn xml_login_user_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<LoginUser version="1.1">
		<userName>9F07915E819A076E2E14169830769D6</userName>
		<password>8EFECD610524A98390F118D2789BE3B</password>
		<userVer>1</userVer>
		</LoginUser>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let lu = parsed.login_user.as_ref().unwrap();
	assert_eq!(lu.user_ver, 1);
	assert_eq!(lu.user_name, "9F07915E819A076E2E14169830769D6");
}

#[test]
fn xml_login_net_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<LoginNet version="1.1">
		<type>LAN</type>
		<udpPort>0</udpPort>
		</LoginNet>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ln = parsed.login_net.as_ref().unwrap();
	assert_eq!(ln.type_, "LAN");
	assert_eq!(ln.udp_port, 0);
}

#[test]
fn xml_device_info_roundtrip_preserves_resolution() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<DeviceInfo version="1.1">
		<resolution>
		<resolutionName>3840*2160</resolutionName>
		<width>3840</width>
		<height>2160</height>
		</resolution>
		</DeviceInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let di = parsed.device_info.as_ref().unwrap();
	let r = di.resolution.as_ref().unwrap();
	assert_eq!(r.width, 3840);
	assert_eq!(r.height, 2160);
	assert_eq!(r.name, "3840*2160");
}

#[test]
fn xml_device_info_tolerates_missing_resolution() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<DeviceInfo version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let di = parsed.device_info.as_ref().unwrap();
	assert!(di.resolution.is_none());
}

#[test]
fn xml_device_info_roundtrip_preserves_device_type() {
	for device_type in ["smart_bell", "wifi_solo_ipc"] {
		let xml = format!(
			r#"<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<DeviceInfo version="1.1">
		<type>{device_type}</type>
		<resolution>
		<resolutionName>2560*1920</resolutionName>
		<width>2560</width>
		<height>1920</height>
		</resolution>
		</DeviceInfo>
		</body>"#
		);
		let parsed = assert_xml_roundtrip_via_bcxml(&xml);
		let di = parsed.device_info.as_ref().unwrap();
		assert_eq!(di.device_type.as_deref(), Some(device_type));
	}
}

#[test]
fn xml_device_info_missing_type_is_none_and_not_serialized() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<DeviceInfo version="1.1">
		<resolution>
		<resolutionName>2560*1920</resolutionName>
		<width>2560</width>
		<height>1920</height>
		</resolution>
		</DeviceInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert!(parsed.device_info.as_ref().unwrap().device_type.is_none());
	let out = parsed.serialize(vec![]).unwrap();
	assert!(!String::from_utf8(out).unwrap().contains("<type>"));
}

#[test]
fn xml_preview_roundtrip_preserves_stream_type() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Preview version="1.1">
		<channelId>0</channelId>
		<handle>0</handle>
		<streamType>mainStream</streamType>
		</Preview>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let p = parsed.preview.as_ref().unwrap();
	assert_eq!(p.handle, 0);
	assert_eq!(p.stream_type.as_deref(), Some("mainStream"));
}

#[test]
fn xml_system_general_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<SystemGeneral version="1.1">
		<timeZone>-25200</timeZone>
		<year>2026</year>
		<month>4</month>
		<day>23</day>
		<hour>14</hour>
		<minute>30</minute>
		<second>15</second>
		<osdFormat>DMY</osdFormat>
		<timeFormat>0</timeFormat>
		<language>English</language>
		<deviceName>FrontYard</deviceName>
		</SystemGeneral>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let sg = parsed.system_general.as_ref().unwrap();
	assert_eq!(sg.time_zone, Some(-25200));
	assert_eq!(sg.year, Some(2026));
	assert_eq!(sg.device_name.as_deref(), Some("FrontYard"));
}

#[test]
fn xml_system_general_empty_version_only() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<SystemGeneral version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let sg = parsed.system_general.as_ref().unwrap();
	assert_eq!(sg.year, None);
	assert_eq!(sg.time_zone, None);
}

#[test]
fn xml_norm_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Norm version="1.1">
		<norm>NTSC</norm>
		</Norm>
		</body>"#
	);
	let _parsed = assert_xml_roundtrip_via_bcxml(xml);
}

#[test]
fn xml_led_state_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<LedState version="1.1">
		<channelId>0</channelId>
		<ledVersion>2</ledVersion>
		<state>auto</state>
		<lightState>open</lightState>
		</LedState>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let led = parsed.led_state.as_ref().unwrap();
	assert_eq!(led.state, "auto");
	assert_eq!(led.light_state, "open");
	assert_eq!(led.led_version, Some(2));
}

#[test]
fn xml_led_state_tolerates_missing_led_version() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<LedState version="1.1">
		<channelId>0</channelId>
		<state>close</state>
		<lightState>close</lightState>
		</LedState>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let led = parsed.led_state.as_ref().unwrap();
	assert_eq!(led.led_version, None);
	assert_eq!(led.state, "close");
}

// ---------------------------------------------------------------------------
// Task 23 batch B — talk / motion / PTZ / floodlight / battery list
// ---------------------------------------------------------------------------

#[test]
fn xml_talk_config_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<TalkConfig version="1.1">
		<channelId>0</channelId>
		<duplex>FDX</duplex>
		<audioStreamMode>followVideoStream</audioStreamMode>
		<audioConfig>
		<audioType>adpcm</audioType>
		<sampleRate>16000</sampleRate>
		<samplePrecision>16</samplePrecision>
		<lengthPerEncoder>512</lengthPerEncoder>
		<soundTrack>mono</soundTrack>
		</audioConfig>
		</TalkConfig>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let tc = parsed.talk_config.as_ref().unwrap();
	assert_eq!(tc.duplex, "FDX");
	assert_eq!(tc.audio_config.sample_rate, 16000);
	assert_eq!(tc.audio_config.audio_type, "adpcm");
}

#[test]
fn xml_talk_ability_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<TalkAbility version="1.1">
		<duplexList>
		<duplex>FDX</duplex>
		</duplexList>
		<audioStreamModeList>
		<audioStreamMode>followVideoStream</audioStreamMode>
		</audioStreamModeList>
		<audioConfigList>
		<audioConfig>
		<audioType>adpcm</audioType>
		<sampleRate>16000</sampleRate>
		<samplePrecision>16</samplePrecision>
		<lengthPerEncoder>512</lengthPerEncoder>
		<soundTrack>mono</soundTrack>
		</audioConfig>
		</audioConfigList>
		</TalkAbility>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ta = parsed.talk_ability.as_ref().unwrap();
	assert_eq!(ta.duplex_list.len(), 1);
	assert_eq!(ta.audio_config_list.len(), 1);
}

#[test]
fn xml_talk_ability_empty_lists() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<TalkAbility version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let ta = parsed.talk_ability.as_ref().unwrap();
	assert!(ta.duplex_list.is_empty());
}

#[test]
fn xml_alarm_event_list_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<AlarmEventList version="1.1">
		<AlarmEvent version="1.1">
		<channelId>0</channelId>
		<status>MD</status>
		<AItype>people</AItype>
		<recording>1</recording>
		<timeStamp>1710000000</timeStamp>
		</AlarmEvent>
		</AlarmEventList>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let list = parsed.alarm_event_list.as_ref().unwrap();
	assert_eq!(list.alarm_events.len(), 1);
	assert_eq!(list.alarm_events[0].status, "MD");
	assert_eq!(list.alarm_events[0].ai_type.as_deref(), Some("people"));
}

#[test]
fn xml_alarm_event_tolerates_missing_ai_type() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<AlarmEventList version="1.1">
		<AlarmEvent>
		<channelId>0</channelId>
		<status>none</status>
		</AlarmEvent>
		</AlarmEventList>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let list = parsed.alarm_event_list.as_ref().unwrap();
	assert_eq!(list.alarm_events[0].ai_type, None);
	assert_eq!(list.alarm_events[0].recording, 0);
}

#[test]
fn xml_ptz_control_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<PtzControl version="1.1">
		<channelId>0</channelId>
		<speed>32</speed>
		<command>left</command>
		</PtzControl>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let pc = parsed.ptz_control.as_ref().unwrap();
	assert_eq!(pc.command, "left");
	assert!((pc.speed - 32.0).abs() < 0.0001);
}

#[test]
fn xml_floodlight_manual_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<FloodlightManual version="1.1">
		<channelId>0</channelId>
		<status>1</status>
		<duration>300</duration>
		</FloodlightManual>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let fm = parsed.floodlight_manual.as_ref().unwrap();
	assert_eq!(fm.status, 1);
	assert_eq!(fm.duration, 300);
}

#[test]
fn xml_battery_list_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<BatteryList version="1.1">
		<BatteryInfo>
		<channelId>0</channelId>
		<chargeStatus>none</chargeStatus>
		<adapterStatus>none</adapterStatus>
		<voltage>4100</voltage>
		<current>0</current>
		<temperature>22</temperature>
		<batteryPercent>90</batteryPercent>
		<lowPower>0</lowPower>
		<batteryVersion>2</batteryVersion>
		</BatteryInfo>
		</BatteryList>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let bl = parsed.battery_list.as_ref().unwrap();
	assert_eq!(bl.battery_info.len(), 1);
	assert_eq!(bl.battery_info[0].battery_percent, 90);
}

#[test]
fn xml_battery_list_empty() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<BatteryList version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	assert!(parsed
		.battery_list
		.as_ref()
		.unwrap()
		.battery_info
		.is_empty());
}

// ---------------------------------------------------------------------------
// Task 23 batch C — ability info / push / link / snap / uid / stream info
// ---------------------------------------------------------------------------

#[test]
fn xml_ability_info_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<AbilityInfo>
		<userName>admin</userName>
		<system>
		<subModule>
		<abilityValue>general_rw, norm_rw, version_ro</abilityValue>
		</subModule>
		</system>
		</AbilityInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ai = parsed.ability_info.as_ref().unwrap();
	assert_eq!(ai.username, "admin");
	let sys = ai.system.as_ref().unwrap();
	assert_eq!(sys.sub_module.len(), 1);
	assert_eq!(
		sys.sub_module[0].ability_value,
		"general_rw, norm_rw, version_ro"
	);
}

#[test]
fn xml_ability_info_with_channel() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<AbilityInfo>
		<userName>admin</userName>
		<PTZ>
		<subModule>
		<channelId>0</channelId>
		<abilityValue>control_rw</abilityValue>
		</subModule>
		</PTZ>
		</AbilityInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ai = parsed.ability_info.as_ref().unwrap();
	let ptz = ai.ptz.as_ref().unwrap();
	assert_eq!(ptz.sub_module[0].channel_id, Some(0));
}

#[test]
fn xml_push_info_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<PushInfo>
		<token>FCMTOKEN1234</token>
		<phoneType>reo_iphone</phoneType>
		<clientID>ABCDEF1234567890ABCDEF1234567890</clientID>
		</PushInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let pi = parsed.push_info.as_ref().unwrap();
	assert_eq!(pi.token, "FCMTOKEN1234");
	assert_eq!(pi.phone_type, "reo_iphone");
}

#[test]
fn xml_link_type_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<LinkType>
		<type>LAN</type>
		</LinkType>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.link_type.as_ref().unwrap().link_type, "LAN");
}

#[test]
fn xml_snap_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Snap version="1.1">
		<channelId>0</channelId>
		<logicChannel>0</logicChannel>
		<time>1710000000</time>
		<fullFrame>0</fullFrame>
		<streamType>main</streamType>
		<fileName>01_20260423140240.jpg</fileName>
		<pictureSize>204800</pictureSize>
		</Snap>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let snap = parsed.snap.as_ref().unwrap();
	assert_eq!(snap.file_name.as_deref(), Some("01_20260423140240.jpg"));
	assert_eq!(snap.picture_size, Some(204800));
}

#[test]
fn xml_snap_tolerates_missing_optional_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Snap version="1.1">
		<channelId>0</channelId>
		<time>0</time>
		</Snap>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let snap = parsed.snap.as_ref().unwrap();
	assert_eq!(snap.file_name, None);
	assert_eq!(snap.picture_size, None);
	assert_eq!(snap.logic_channel, None);
}

#[test]
fn xml_uid_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Uid version="1.1">
		<uid>ABCDEF0123456789</uid>
		</Uid>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.uid.as_ref().unwrap().uid, "ABCDEF0123456789");
}

#[test]
fn xml_stream_info_list_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<StreamInfoList version="1.1">
		<StreamInfo>
		<channelBits>1</channelBits>
		<encodeTable>
		<type>mainStream</type>
		<resolution>
		<width>2560</width>
		<height>1440</height>
		</resolution>
		<defaultFramerate>25</defaultFramerate>
		<defaultBitrate>4096</defaultBitrate>
		<framerateTable>25,22,20,18,16,15,12,10,8,6,4,2</framerateTable>
		<bitrateTable>1024,1536,2048,3072,4096</bitrateTable>
		</encodeTable>
		</StreamInfo>
		</StreamInfoList>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let sil = parsed.stream_info_list.as_ref().unwrap();
	assert_eq!(sil.stream_infos.len(), 1);
	let et = &sil.stream_infos[0].encode_tables[0];
	assert_eq!(et.name, "mainStream");
	assert_eq!(et.resolution.width, 2560);
	assert_eq!(et.default_bitrate, 4096);
}

// ---------------------------------------------------------------------------
// Task 23 batch D — port configs + email + user list
// ---------------------------------------------------------------------------

#[test]
fn xml_server_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<ServerPort version="1.1">
		<serverPort>9000</serverPort>
		<enable>1</enable>
		</ServerPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let sp = parsed.server_port.as_ref().unwrap();
	assert_eq!(sp.port, 9000);
	assert_eq!(sp.enable, Some(1));
}

#[test]
fn xml_server_port_tolerates_missing_enable() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<ServerPort version="1.1">
		<serverPort>9000</serverPort>
		</ServerPort>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	assert_eq!(parsed.server_port.as_ref().unwrap().enable, None);
}

#[test]
fn xml_http_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<HttpPort version="1.1">
		<httpPort>80</httpPort>
		<enable>1</enable>
		</HttpPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.http_port.as_ref().unwrap().port, 80);
}

#[test]
fn xml_https_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<HttpsPort version="1.1">
		<httpsPort>443</httpsPort>
		<enable>0</enable>
		</HttpsPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.https_port.as_ref().unwrap().port, 443);
}

#[test]
fn xml_rtsp_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<RtspPort version="1.1">
		<rtspPort>554</rtspPort>
		<enable>1</enable>
		</RtspPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.rtsp_port.as_ref().unwrap().port, 554);
}

#[test]
fn xml_rtmp_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<RtmpPort version="1.1">
		<rtmpPort>1935</rtmpPort>
		<enable>1</enable>
		</RtmpPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.rtmp_port.as_ref().unwrap().port, 1935);
}

#[test]
fn xml_rtmp_port_tolerates_missing_enable() {
	// Some cameras report rtmp port without an enable flag.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<RtmpPort version="1.1">
		<rtmpPort>1935</rtmpPort>
		</RtmpPort>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	assert_eq!(parsed.rtmp_port.as_ref().unwrap().enable, None);
}

#[test]
fn xml_onvif_port_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<OnvifPort version="1.1">
		<onvifPort>8000</onvifPort>
		<enable>1</enable>
		</OnvifPort>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	assert_eq!(parsed.onvif_port.as_ref().unwrap().port, 8000);
}

#[test]
fn xml_email_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Email version="1.1">
		<smtpServer>smtp.example.com</smtpServer>
		<userName>camera@example.com</userName>
		<password>secret</password>
		<address1>dest@example.com</address1>
		<address2></address2>
		<address3></address3>
		<smtpPort>465</smtpPort>
		<sendNickname>CameraAlerts</sendNickname>
		<attachment>1</attachment>
		<attachmentType>picture</attachmentType>
		<textType>withText</textType>
		<ssl>1</ssl>
		<interval>30</interval>
		<senderMaxLen>127</senderMaxLen>
		</Email>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let e = parsed.email.as_ref().unwrap();
	assert_eq!(e.smtp_server, "smtp.example.com");
	assert_eq!(e.smtp_port, 465);
	assert_eq!(e.attachment_type.as_deref(), Some("picture"));
	assert_eq!(e.sender_max_len, Some(127));
}

#[test]
fn xml_email_tolerates_missing_optional_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Email version="1.1">
		<smtpServer>smtp.example.com</smtpServer>
		</Email>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let e = parsed.email.as_ref().unwrap();
	assert_eq!(e.smtp_server, "smtp.example.com");
	assert_eq!(e.attachment_type, None);
	assert_eq!(e.sender_max_len, None);
}

#[test]
fn xml_email_task_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<EmailTask version="1.1">
		<channelId>0</channelId>
		<enable>1</enable>
		<ScheduleList>
		<Schedule>
		<alarmType>MD</alarmType>
		<timeBlockList>
		<timeBlock>
		<enable>1</enable>
		<weekDay>Monday</weekDay>
		<beginHour>0</beginHour>
		<endHour>23</endHour>
		</timeBlock>
		</timeBlockList>
		</Schedule>
		</ScheduleList>
		</EmailTask>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let et = parsed.email_task.as_ref().unwrap();
	assert_eq!(et.enable, 1);
	assert!(et.schedule_list.is_some());
}

#[test]
fn xml_user_list_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<UserList version="1.1">
		<User>
		<userName>admin</userName>
		<userId>0</userId>
		<userLevel>1</userLevel>
		<loginState>1</loginState>
		<userSetState>none</userSetState>
		</User>
		</UserList>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ul = parsed.user_list.as_ref().unwrap();
	let users = ul.user_list.as_ref().unwrap();
	assert_eq!(users.len(), 1);
	assert_eq!(users[0].user_name, "admin");
	assert_eq!(users[0].user_level, 1);
	assert_eq!(users[0].login_state, Some(1));
}

#[test]
fn xml_user_list_empty_parses_cleanly() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<UserList version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	assert!(parsed.user_list.is_some());
}

// ---------------------------------------------------------------------------
// Task 23 batch E — floodlight task, PTZ zoom/focus, audio, support
// ---------------------------------------------------------------------------

#[test]
fn xml_floodlight_task_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<FloodlightTask version="1.1">
		<channel>0</channel>
		<alarmMode>1</alarmMode>
		<enable>1</enable>
		<lastAlarmMode>2</lastAlarmMode>
		<preview_auto>0</preview_auto>
		<duration>300</duration>
		<brightness_cur>100</brightness_cur>
		<brightness_max>100</brightness_max>
		<brightness_min>1</brightness_min>
		<schedule>
		<startHour>18</startHour>
		<startMin>0</startMin>
		<endHour>6</endHour>
		<endMin>0</endMin>
		</schedule>
		<lightSensThreshold>
		<min>1000</min>
		<max>2300</max>
		<lightCur>1000</lightCur>
		<darkCur>1900</darkCur>
		<lightDef>1000</lightDef>
		<darkDef>1900</darkDef>
		</lightSensThreshold>
		<FloodlightScheduleList>
		<maxNum>32</maxNum>
		</FloodlightScheduleList>
		<nightLongViewMultiBrightness>
		<enable>0</enable>
		<alarmBrightness>
		<min>1</min>
		<max>100</max>
		<cur>100</cur>
		<def>100</def>
		</alarmBrightness>
		<alarmDelay>
		<min>5</min>
		<max>600</max>
		<cur>10</cur>
		<def>10</def>
		</alarmDelay>
		</nightLongViewMultiBrightness>
		<detectType>none</detectType>
		</FloodlightTask>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let ft = parsed.floodlight_task.as_ref().unwrap();
	assert_eq!(ft.enable, 1);
	assert_eq!(ft.duration, 300);
	assert_eq!(ft.brightness_max, Some(100));
	assert_eq!(ft.schedule.start_hour, 18);
	assert_eq!(ft.light_sens_threshold.light_cur, 1000);
	assert_eq!(ft.floodlight_schedule_list.max_num, 32);
}

#[test]
fn xml_floodlight_task_tolerates_missing_brightness_range() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<FloodlightTask version="1.1">
		<channel>0</channel>
		<alarmMode>1</alarmMode>
		<enable>1</enable>
		<lastAlarmMode>2</lastAlarmMode>
		<preview_auto>0</preview_auto>
		<duration>300</duration>
		<brightness_cur>100</brightness_cur>
		<detectType>none</detectType>
		</FloodlightTask>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let ft = parsed.floodlight_task.as_ref().unwrap();
	assert_eq!(ft.brightness_max, None);
	assert_eq!(ft.brightness_min, None);
}

#[test]
fn xml_ptz_zoom_focus_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<PtzZoomFocus version="1.1">
		<channelId>0</channelId>
		<zoom>
		<maxPos>4000</maxPos>
		<minPos>0</minPos>
		<curPos>2500</curPos>
		</zoom>
		<focus>
		<maxPos>4000</maxPos>
		<minPos>0</minPos>
		<curPos>3000</curPos>
		</focus>
		</PtzZoomFocus>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let pzf = parsed.ptz_zoom_focus.as_ref().unwrap();
	assert_eq!(pzf.zoom.max_pos, 4000);
	assert_eq!(pzf.focus.cur_pos, 3000);
}

#[test]
fn xml_start_zoom_focus_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<StartZoomFocus version="1.1">
		<channelId>0</channelId>
		<command>zoomPos</command>
		<movePos>2994</movePos>
		</StartZoomFocus>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let szf = parsed.start_zoom_focus.as_ref().unwrap();
	assert_eq!(szf.command, "zoomPos");
	assert_eq!(szf.move_pos, 2994);
}

#[test]
fn xml_audio_play_info_roundtrip_preserves_all_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<audioPlayInfo>
		<channelId>0</channelId>
		<playMode>0</playMode>
		<playDuration>0</playDuration>
		<playTimes>1</playTimes>
		<onOff>1</onOff>
		</audioPlayInfo>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let api = parsed.audio_play_info.as_ref().unwrap();
	assert_eq!(api.play_times, 1);
	assert_eq!(api.on_off, 1);
}

#[test]
fn xml_support_roundtrip_preserves_common_fields() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<IOInputPortNum>0</IOInputPortNum>
		<IOOutputPortNum>0</IOOutputPortNum>
		<diskNum>1</diskNum>
		<channelNum>1</channelNum>
		<audioNum>1</audioNum>
		<ptzMode>pt</ptzMode>
		<audioTalk>1</audioTalk>
		<email>1</email>
		<rtsp>1</rtsp>
		<onvif>1</onvif>
		<largeBattery>1</largeBattery>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let s = parsed.support.as_ref().unwrap();
	assert_eq!(s.channel_num, Some(1));
	assert_eq!(s.ptz_mode.as_deref(), Some("pt"));
	assert_eq!(s.rtsp, Some(1));
	assert_eq!(s.large_battery, Some(1));
}

#[test]
fn xml_support_minimal_version_only_parses() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let s = parsed.support.as_ref().unwrap();
	assert_eq!(s.channel_num, None);
	assert!(s.items.is_empty());
}

#[test]
fn xml_support_with_smart_home_roundtrip() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<smartHome>
		<version>1</version>
		<item>
		<name>googleHome</name>
		<ver>1</ver>
		</item>
		<item>
		<name>amazonAlexa</name>
		<ver>1</ver>
		</item>
		</smartHome>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let sh = parsed
		.support
		.as_ref()
		.unwrap()
		.smart_home
		.as_ref()
		.unwrap();
	assert_eq!(sh.items.len(), 2);
	assert_eq!(sh.items[0].name, "googleHome");
}

#[test]
fn xml_rf_alarm_cfg_tolerates_missing_enable_field() {
	// enable uses #[serde(default)]; a truly empty rfAlarmCfg must still parse.
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<rfAlarmCfg version="1.1"/>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).expect("parse empty rfAlarmCfg");
	let cfg = parsed.rf_alarm_cfg.as_ref().unwrap();
	assert_eq!(cfg.enable, 0);
	assert_eq!(cfg.rf_id, None);
}

// ---------------------------------------------------------------------------
// Task 23 batch F — Extension (standalone serialize surface)
// ---------------------------------------------------------------------------

fn extension_roundtrip_assert(ext: &Extension) {
	// Extension has its own serialize / try_parse (separate from BcXml).
	let bytes = Extension::default();
	let _ = bytes; // keep compiler happy when field missing
	let bytes = ext.clone_for_test().serialize(vec![]).expect("ser");
	let reparsed = Extension::try_parse(bytes.as_slice()).expect("reparse");
	assert_eq!(ext, &reparsed, "extension round-trip mismatch");
}

trait ExtClone {
	fn clone_for_test(&self) -> Extension;
}
impl ExtClone for Extension {
	fn clone_for_test(&self) -> Extension {
		Extension {
			version: self.version.clone(),
			binary_data: self.binary_data,
			user_name: self.user_name.clone(),
			token: self.token.clone(),
			channel_id: self.channel_id,
			rf_id: self.rf_id,
			check_pos: self.check_pos,
			check_value: self.check_value,
			encrypt_len: self.encrypt_len,
		}
	}
}

#[test]
fn xml_extension_roundtrip_with_every_field() {
	let ext = Extension {
		version: "1.1".to_string(),
		binary_data: Some(1),
		user_name: Some("admin".to_string()),
		token: Some("system, network".to_string()),
		channel_id: Some(0),
		rf_id: Some(0),
		check_pos: Some(16),
		check_value: Some(42),
		encrypt_len: Some(256),
	};
	extension_roundtrip_assert(&ext);
}

#[test]
fn xml_extension_roundtrip_minimal() {
	let ext = Extension::default();
	extension_roundtrip_assert(&ext);
}

#[test]
fn xml_extension_roundtrip_binary_marker_only() {
	let ext = Extension {
		version: "1.1".to_string(),
		binary_data: Some(1),
		user_name: None,
		token: None,
		channel_id: None,
		rf_id: None,
		check_pos: None,
		check_value: None,
		encrypt_len: None,
	};
	extension_roundtrip_assert(&ext);
}

// ---------------------------------------------------------------------------
// Task 23 batch G — SupportItem (per-channel support flags)
// ---------------------------------------------------------------------------

#[test]
fn xml_support_item_roundtrip_preserves_common_flags() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<item>
		<chnID>0</chnID>
		<ptzType>2</ptzType>
		<rfCfg>1</rfCfg>
		<battery>1</battery>
		<ledCtrl>1</ledCtrl>
		<ptzControl>1</ptzControl>
		<ptzPreset>1</ptzPreset>
		<motion>1</motion>
		<snap>1</snap>
		<h264Profile>7</h264Profile>
		</item>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let s = parsed.support.as_ref().unwrap();
	assert_eq!(s.items.len(), 1);
	let it = &s.items[0];
	assert_eq!(it.chn_id, 0);
	assert_eq!(it.ptz_type, Some(2));
	assert_eq!(it.battery, Some(1));
	assert_eq!(it.ptz_preset, Some(1));
	assert_eq!(it.h264_profile, Some(7));
}

#[test]
fn xml_support_item_roundtrip_preserves_doorbell_version() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<item>
		<chnID>0</chnID>
		<aitype>1</aitype>
		<doorbellVersion>16</doorbellVersion>
		</item>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let it = &parsed.support.as_ref().unwrap().items[0];
	assert_eq!(it.doorbell_version, Some(16));
	assert_eq!(it.ai_type, Some(1));
}

#[test]
fn xml_support_item_missing_doorbell_version_is_none_and_not_serialized() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<item>
		<chnID>0</chnID>
		<battery>1</battery>
		</item>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let it = &parsed.support.as_ref().unwrap().items[0];
	assert_eq!(it.doorbell_version, None);
	let out = parsed.serialize(vec![]).unwrap();
	assert!(!String::from_utf8(out).unwrap().contains("doorbellVersion"));
}

#[test]
fn xml_support_item_tolerates_missing_optional_flags() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<item>
		<chnID>1</chnID>
		</item>
		</Support>
		</body>"#
	);
	let parsed = BcXml::try_parse(xml.as_bytes()).unwrap();
	let it = &parsed.support.as_ref().unwrap().items[0];
	assert_eq!(it.chn_id, 1);
	assert_eq!(it.ptz_type, None);
	assert_eq!(it.battery, None);
}

#[test]
fn xml_support_item_multi_channel_roundtrip() {
	let xml = indoc!(
		r#"
		<?xml version="1.0" encoding="UTF-8" ?>
		<body>
		<Support version="1.1">
		<item>
		<chnID>0</chnID>
		<battery>1</battery>
		</item>
		<item>
		<chnID>1</chnID>
		<battery>0</battery>
		</item>
		</Support>
		</body>"#
	);
	let parsed = assert_xml_roundtrip_via_bcxml(xml);
	let s = parsed.support.as_ref().unwrap();
	assert_eq!(s.items.len(), 2);
	assert_eq!(s.items[0].battery, Some(1));
	assert_eq!(s.items[1].battery, Some(0));
}

// Property test: `BcXml::try_parse` and `Extension::try_parse` must
// absorb any byte sequence the post-decryption payload buffer can
// contain without panicking. ~2270 LOC of `serde` deserializers — the
// largest untrusted-input parse surface in the crate. Compromised
// firmware, on-path attackers, or relay-spoofing peers can substitute
// any bytes for the XML payload after AES-CFB decrypt.
mod proptest_arbitrary_xml {
	use super::*;
	use proptest::prelude::*;

	proptest! {
		#![proptest_config(ProptestConfig {
			cases: 1024,
			..ProptestConfig::default()
		})]

		#[test]
		fn bcxml_try_parse_never_panics_on_arbitrary_bytes(
			bytes in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			// `try_parse` must never panic; quick_xml errors return Err.
			let _ = BcXml::try_parse(bytes.as_slice());
		}

		#[test]
		fn extension_try_parse_never_panics_on_arbitrary_bytes(
			bytes in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			let _ = Extension::try_parse(bytes.as_slice());
		}

		#[test]
		fn bcxml_try_parse_with_xml_decl_prefix_never_panics(
			tail in proptest::collection::vec(any::<u8>(), 0..4096),
		) {
			// Bias toward "looks like XML": the parser walks deeper into
			// the per-element branches before erroring. Catches the
			// hot-path serde-deserializer code paths.
			let mut bytes = b"<?xml version=\"1.0\"?>".to_vec();
			bytes.extend_from_slice(&tail);
			let _ = BcXml::try_parse(bytes.as_slice());
		}
	}
}
