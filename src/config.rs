use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub state_file: Option<StateFileConfig>,
    #[serde(default)]
    pub api: Option<ApiConfig>,
    #[serde(default)]
    pub total_quota: Option<ByteSize>,
    /// Single-user shorthand: top-level rules become a user named
    /// "default". Mutually exclusive with `users`.
    #[serde(default)]
    pub rules: Vec<Rule>,
    #[serde(default)]
    pub users: Vec<UserConfig>,
}

/// Optional usage persistence. Omitting the whole section keeps counters
/// in memory only, so they reset on restart.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateFileConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_state_path")]
    pub path: PathBuf,
    #[serde(default = "default_flush_secs")]
    pub flush_secs: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    pub listen: SocketAddr,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub name: String,
    #[serde(default)]
    pub quota: Option<ByteSize>,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub listen: SocketAddr,
    pub target: String,
    /// Backup targets tried in priority order when the target above is
    /// unreachable. Accepts a single string or a list of strings.
    #[serde(default, deserialize_with = "string_or_strings")]
    pub fallback: Vec<String>,
    /// Health check interval (seconds) for rules with fallbacks; also the
    /// retry cooldown for UDP-only rules, which cannot be probed.
    #[serde(default = "default_check_secs")]
    pub check_secs: u64,
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub tag: Option<String>,
}

fn string_or_strings<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a target string or a list of target strings")
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Vec<String>, E> {
            Ok(vec![v.to_owned()])
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Vec<String>, A::Error> {
            let mut targets = Vec::new();
            while let Some(target) = seq.next_element()? {
                targets.push(target);
            }
            Ok(targets)
        }
    }

    deserializer.deserialize_any(Visitor)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Both,
    Tcp,
    Udp,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Protocol::Both => "tcp+udp",
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        })
    }
}

impl Protocol {
    pub fn tcp(self) -> bool {
        matches!(self, Protocol::Both | Protocol::Tcp)
    }

    pub fn udp(self) -> bool {
        matches!(self, Protocol::Both | Protocol::Udp)
    }
}

/// Accepts an IP literal with port, or a `host:port` shape whose hostname is
/// resolved at connect time (see `dns::resolve`).
fn check_target(target: &str) -> Result<(), String> {
    if target.parse::<SocketAddr>().is_ok() {
        return Ok(());
    }
    crate::dns::split_target(target).map(|_| ())
}

fn default_state_path() -> PathBuf {
    PathBuf::from("state.db")
}

fn default_flush_secs() -> u64 {
    10
}

fn default_true() -> bool {
    true
}

fn default_check_secs() -> u64 {
    10
}

/// A byte count that deserializes from either a plain integer (bytes) or a
/// human-readable string like "500MB" / "10GB" / "1.5TB" (1024-based).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a byte count (integer) or a size string like \"10GB\"")
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom("byte size cannot be negative"))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ByteSize, E> {
                parse_size(v).map(ByteSize).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Parses "1024", "500MB", "10 GiB", "1.5TB" etc. into bytes (1024-based).
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num.parse().map_err(|_| format!("invalid size: {s:?}"))?;
    let unit = unit.trim().to_ascii_uppercase();
    let mult: u64 = match unit.as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1 << 10,
        "M" | "MB" | "MIB" => 1 << 20,
        "G" | "GB" | "GIB" => 1 << 30,
        "T" | "TB" | "TIB" => 1u64 << 40,
        _ => return Err(format!("unknown size unit {unit:?} in {s:?}")),
    };
    let bytes = num * mult as f64;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u64::MAX as f64 {
        return Err(format!("size out of range: {s:?}"));
    }
    Ok(bytes as u64)
}

pub fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        return format!("{bytes}B");
    }
    format!("{value:.2}{}", UNITS[unit])
}

pub fn load(path: &Path) -> Result<Config, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read config {}: {e}", path.display()))?;
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let config: Config = if ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml") {
        serde_yaml_ng::from_str(&raw)
            .map_err(|e| format!("invalid config {}: {e}", path.display()))?
    } else {
        // JSON5: a JSON superset with comments, trailing commas, unquoted
        // keys, and single-quoted strings.
        json5::from_str(&raw).map_err(|e| format!("invalid config {}: {e}", path.display()))?
    };
    finalize(config)
}

/// Folds the single-user shorthand into `users`, then validates.
fn finalize(mut config: Config) -> Result<Config, String> {
    if !config.rules.is_empty() {
        if !config.users.is_empty() {
            return Err("top-level rules and users cannot both be set".into());
        }
        config.users.push(UserConfig {
            name: "default".into(),
            quota: None,
            rules: std::mem::take(&mut config.rules),
        });
    }
    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<(), String> {
    if config.users.is_empty() {
        return Err("config has no users or rules".into());
    }
    if let Some(state) = &config.state_file
        && state.flush_secs == 0
    {
        return Err("state_file flush_secs must be at least 1".into());
    }

    let mut names = HashSet::new();
    let mut tcp_listens = HashSet::new();
    let mut udp_listens = HashSet::new();
    if let Some(api) = &config.api {
        tcp_listens.insert(api.listen);
    }

    for user in &config.users {
        if user.name.is_empty() {
            return Err("user name cannot be empty".into());
        }
        if !names.insert(&user.name) {
            return Err(format!("duplicate user name {:?}", user.name));
        }
        for rule in &user.rules {
            if !rule.enabled {
                continue;
            }
            if rule.target.is_empty() {
                return Err(format!("user {:?}: rule target cannot be empty", user.name));
            }
            if rule.fallback.iter().any(String::is_empty) {
                return Err(format!(
                    "user {:?}: rule fallback target cannot be empty",
                    user.name
                ));
            }
            for target in std::iter::once(&rule.target).chain(&rule.fallback) {
                check_target(target).map_err(|e| format!("user {:?}: {e}", user.name))?;
            }
            if rule.check_secs == 0 {
                return Err(format!(
                    "user {:?}: rule check_secs must be at least 1",
                    user.name
                ));
            }
            if rule.tag.as_deref() == Some("") {
                return Err(format!("user {:?}: rule tag cannot be empty", user.name));
            }
            if rule.protocol.tcp() && !tcp_listens.insert(rule.listen) {
                return Err(format!("duplicate TCP listen address {}", rule.listen));
            }
            if rule.protocol.udp() && !udp_listens.insert(rule.listen) {
                return Err(format!("duplicate UDP listen address {}", rule.listen));
            }
        }
    }

    Ok(())
}

/// Returns the effective quota (bytes) per user: an explicit `quota` wins;
/// users without one split `total_quota` evenly. With neither, the user is
/// unlimited (`None`): usage is tracked but never enforced.
pub fn resolve_quotas(config: &Config) -> HashMap<String, Option<u64>> {
    let unset = config.users.iter().filter(|u| u.quota.is_none()).count() as u64;
    let share = config
        .total_quota
        .filter(|_| unset > 0)
        .map(|ByteSize(total)| total / unset);

    config
        .users
        .iter()
        .map(|u| (u.name.clone(), u.quota.map(|q| q.0).or(share)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_config(json: &str) -> Result<Config, String> {
        let config: Config = json5::from_str(json).map_err(|e| e.to_string())?;
        finalize(config)
    }

    #[test]
    fn parse_size_variants() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("500MB").unwrap(), 500 << 20);
        assert_eq!(parse_size("10GB").unwrap(), 10 << 30);
        assert_eq!(parse_size("10 GiB").unwrap(), 10 << 30);
        assert_eq!(parse_size("1tb").unwrap(), 1 << 40);
        assert_eq!(
            parse_size("1.5GB").unwrap(),
            (1.5 * (1u64 << 30) as f64) as u64
        );
        assert!(parse_size("10XB").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("").is_err());
    }

    #[test]
    fn format_size_auto_units() {
        assert_eq!(format_size(0), "0B");
        assert_eq!(format_size(413), "413B");
        assert_eq!(format_size(11568), "11.30KB");
        assert_eq!(format_size(52428800), "50.00MB");
        assert_eq!(format_size(875099586560), "815.00GB");
        assert_eq!(format_size(52417232), "49.99MB");
        assert_eq!(format_size(2 << 40), "2.00TB");
    }

    #[test]
    fn quota_from_number_or_string() {
        let config = parse_config(
            r#"{
                "users": [
                    {"name": "a", "quota": 1048576, "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]},
                    {"name": "b", "quota": "1MB", "rules": [{"listen": "0.0.0.0:2", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap();
        let quotas = resolve_quotas(&config);
        assert_eq!(quotas["a"], Some(1 << 20));
        assert_eq!(quotas["b"], Some(1 << 20));
    }

    #[test]
    fn total_quota_split_evenly_among_unset() {
        let config = parse_config(
            r#"{
                "total_quota": "100GB",
                "users": [
                    {"name": "a", "quota": "30GB", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]},
                    {"name": "b", "rules": [{"listen": "0.0.0.0:2", "target": "x:1"}]},
                    {"name": "c", "rules": [{"listen": "0.0.0.0:3", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap();
        let quotas = resolve_quotas(&config);
        assert_eq!(quotas["a"], Some(30u64 << 30));
        assert_eq!(quotas["b"], Some(50u64 << 30));
        assert_eq!(quotas["c"], Some(50u64 << 30));
    }

    #[test]
    fn missing_quota_and_total_means_unlimited() {
        let config = parse_config(
            r#"{
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]},
                    {"name": "b", "quota": "1MB", "rules": [{"listen": "0.0.0.0:2", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap();
        let quotas = resolve_quotas(&config);
        assert_eq!(quotas["a"], None);
        assert_eq!(quotas["b"], Some(1 << 20));
    }

    #[test]
    fn duplicate_user_name_is_error() {
        let err = parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]},
                    {"name": "a", "rules": [{"listen": "0.0.0.0:2", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate user"), "{err}");
    }

    #[test]
    fn duplicate_listen_conflicts_by_protocol() {
        // Same port on tcp and udp is fine.
        parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "protocol": "tcp"}]},
                    {"name": "b", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "protocol": "udp"}]}
                ]
            }"#,
        )
        .unwrap();
        // Default protocol "both" collides with an existing tcp listener.
        let err = parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "protocol": "tcp"}]},
                    {"name": "b", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate TCP"), "{err}");
    }

    #[test]
    fn rule_tag_parses_and_defaults_none() {
        let config = parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [
                        {"listen": "0.0.0.0:1", "target": "x:1", "tag": "web"},
                        {"listen": "0.0.0.0:2", "target": "x:1"}
                    ]}
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(config.users[0].rules[0].tag.as_deref(), Some("web"));
        assert_eq!(config.users[0].rules[1].tag, None);
    }

    #[test]
    fn empty_rule_tag_is_error() {
        let err = parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "tag": ""}]}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("tag cannot be empty"), "{err}");
    }

    #[test]
    fn disabled_rules_do_not_conflict() {
        parse_config(
            r#"{
                "total_quota": 1,
                "users": [
                    {"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "enabled": false}]},
                    {"name": "b", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap();
    }

    #[test]
    fn fallback_accepts_string_or_list() {
        let config = parse_config(
            r#"{
                "rules": [
                    {"listen": "0.0.0.0:1", "target": "x:1", "fallback": "y:1"},
                    {"listen": "0.0.0.0:2", "target": "x:1", "fallback": ["y:1", "z:1"]},
                    {"listen": "0.0.0.0:3", "target": "x:1"}
                ]
            }"#,
        )
        .unwrap();
        let rules = &config.users[0].rules;
        assert_eq!(rules[0].fallback, vec!["y:1"]);
        assert_eq!(rules[1].fallback, vec!["y:1", "z:1"]);
        assert!(rules[2].fallback.is_empty());
        assert_eq!(rules[0].check_secs, 10);
    }

    #[test]
    fn malformed_target_is_error() {
        for (target, want) in [
            ("example.com", "missing a port"),
            ("example.com:99999", "invalid port"),
        ] {
            let err = parse_config(&format!(
                r#"{{"rules": [{{"listen": "0.0.0.0:1", "target": "{target}"}}]}}"#,
            ))
            .unwrap_err();
            assert!(err.contains(want), "{err}");
        }
        // Domains and IP literals both pass.
        parse_config(
            r#"{"rules": [{"listen": "0.0.0.0:1", "target": "example.com:80", "fallback": "[::1]:80"}]}"#,
        )
        .unwrap();
    }

    #[test]
    fn empty_fallback_entry_is_error() {
        let err = parse_config(
            r#"{"rules": [{"listen": "0.0.0.0:1", "target": "x:1", "fallback": ["y:1", ""]}]}"#,
        )
        .unwrap_err();
        assert!(err.contains("fallback target cannot be empty"), "{err}");
    }

    #[test]
    fn zero_check_secs_is_error() {
        let err = parse_config(
            r#"{"rules": [{"listen": "0.0.0.0:1", "target": "x:1", "check_secs": 0}]}"#,
        )
        .unwrap_err();
        assert!(err.contains("check_secs"), "{err}");
    }

    #[test]
    fn top_level_rules_make_default_user() {
        let config =
            parse_config(r#"{"rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#).unwrap();
        assert_eq!(config.users.len(), 1);
        assert_eq!(config.users[0].name, "default");
        assert_eq!(config.users[0].rules.len(), 1);
        // Without quota/total_quota the implicit user is unlimited.
        assert_eq!(resolve_quotas(&config)["default"], None);

        // total_quota becomes the implicit user's quota.
        let config = parse_config(
            r#"{"total_quota": "1MB", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#,
        )
        .unwrap();
        assert_eq!(resolve_quotas(&config)["default"], Some(1 << 20));
    }

    #[test]
    fn state_file_defaults_to_none() {
        let config =
            parse_config(r#"{"rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#).unwrap();
        assert!(config.state_file.is_none());
    }

    #[test]
    fn empty_state_file_section_uses_defaults() {
        let config = parse_config(
            r#"{"state_file": {}, "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#,
        )
        .unwrap();
        let state = config.state_file.unwrap();
        assert!(state.enabled);
        assert_eq!(state.path, PathBuf::from("state.db"));
        assert_eq!(state.flush_secs, 10);
    }

    #[test]
    fn state_file_disabled_and_zero_flush_secs() {
        let config = parse_config(
            r#"{"state_file": {"enabled": false}, "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#,
        )
        .unwrap();
        assert!(!config.state_file.unwrap().enabled);

        let err = parse_config(
            r#"{"state_file": {"flush_secs": 0}, "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}"#,
        )
        .unwrap_err();
        assert!(err.contains("flush_secs"), "{err}");
    }

    #[test]
    fn top_level_rules_and_users_conflict() {
        let err = parse_config(
            r#"{
                "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}],
                "users": [{"name": "a", "rules": [{"listen": "0.0.0.0:2", "target": "x:1"}]}]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("cannot both be set"), "{err}");
    }

    #[test]
    fn empty_config_is_error() {
        let err = parse_config(r#"{}"#).unwrap_err();
        assert!(err.contains("no users or rules"), "{err}");
    }

    #[test]
    fn yaml_config_parses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        std::fs::write(
            &path,
            r#"
total_quota: 100GB
api:
  listen: 127.0.0.1:7070
  token: changeme
users:
  - name: a
    quota: 1048576
    rules:
      - listen: 0.0.0.0:1
        target: x:1
        fallback:
          - y:1
          - z:1
        tag: web
  - name: b
    quota: 1MB
    rules:
      - listen: 0.0.0.0:2
        target: x:1
        protocol: udp
        enabled: false
"#,
        )
        .unwrap();
        let config = load(&path).unwrap();
        let quotas = resolve_quotas(&config);
        assert_eq!(quotas["a"], Some(1 << 20));
        assert_eq!(quotas["b"], Some(1 << 20));
        assert_eq!(config.users[0].rules[0].tag.as_deref(), Some("web"));
        assert_eq!(config.users[0].rules[0].fallback, vec!["y:1", "z:1"]);
        assert_eq!(config.users[1].rules[0].protocol, Protocol::Udp);
        assert!(!config.users[1].rules[0].enabled);
    }

    #[test]
    fn load_dispatches_on_extension() {
        let dir = tempfile::tempdir().unwrap();
        let json = r#"{"total_quota": 1, "users": [{"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}]}"#;

        // JSON content under a .json name parses; the same content under a
        // .yml name also parses (YAML is a superset of JSON).
        for name in ["config.json", "config.yml"] {
            let path = dir.path().join(name);
            std::fs::write(&path, json).unwrap();
            load(&path).unwrap();
        }

        // YAML-only syntax is rejected when the file is not .yaml/.yml.
        let yaml = "total_quota: 1\nusers:\n  - name: a\n    rules:\n      - listen: 0.0.0.0:1\n        target: x:1\n";
        let bad = dir.path().join("config.conf");
        std::fs::write(&bad, yaml).unwrap();
        assert!(load(&bad).unwrap_err().contains("invalid config"));
        let good = dir.path().join("config.yaml");
        std::fs::write(&good, yaml).unwrap();
        load(&good).unwrap();
    }

    #[test]
    fn json5_syntax_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                // line comment
                total_quota: '1MB', /* block comment */
                rules: [
                    {listen: "0.0.0.0:1", target: "x:1"},
                ],
            }"#,
        )
        .unwrap();
        let config = load(&path).unwrap();
        assert_eq!(config.total_quota, Some(ByteSize(1 << 20)));
    }

    #[test]
    fn api_listen_conflicts_with_tcp_rule() {
        let err = parse_config(
            r#"{
                "total_quota": 1,
                "api": {"listen": "127.0.0.1:7070"},
                "users": [
                    {"name": "a", "rules": [{"listen": "127.0.0.1:7070", "target": "x:1"}]}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate TCP"), "{err}");
    }
}
