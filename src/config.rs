use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_state_db")]
    pub state_db: PathBuf,
    #[serde(default = "default_flush_secs")]
    pub state_flush_secs: u64,
    #[serde(default)]
    pub api: Option<ApiConfig>,
    #[serde(default)]
    pub total_quota: Option<ByteSize>,
    pub users: Vec<UserConfig>,
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
    #[serde(default)]
    pub protocol: Protocol,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub tag: Option<String>,
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

fn default_state_db() -> PathBuf {
    PathBuf::from("state.db")
}

fn default_flush_secs() -> u64 {
    10
}

fn default_true() -> bool {
    true
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
    let num: f64 = num
        .parse()
        .map_err(|_| format!("invalid size: {s:?}"))?;
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
        serde_json::from_str(&raw)
            .map_err(|e| format!("invalid config {}: {e}", path.display()))?
    };
    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<(), String> {
    if config.users.is_empty() {
        return Err("config has no users".into());
    }
    if config.state_flush_secs == 0 {
        return Err("state_flush_secs must be at least 1".into());
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

    resolve_quotas(config).map(|_| ())
}

/// Returns the effective quota (bytes) per user: an explicit `quota` wins;
/// users without one split `total_quota` evenly.
pub fn resolve_quotas(config: &Config) -> Result<HashMap<String, u64>, String> {
    let unset: Vec<&UserConfig> = config
        .users
        .iter()
        .filter(|u| u.quota.is_none())
        .collect();

    let share = if unset.is_empty() {
        0
    } else {
        match config.total_quota {
            Some(ByteSize(total)) => total / unset.len() as u64,
            None => {
                let names: Vec<&str> = unset.iter().map(|u| u.name.as_str()).collect();
                return Err(format!(
                    "users {names:?} have no quota and no total_quota is set"
                ));
            }
        }
    };

    Ok(config
        .users
        .iter()
        .map(|u| (u.name.clone(), u.quota.map_or(share, |q| q.0)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_config(json: &str) -> Result<Config, String> {
        let config: Config =
            serde_json::from_str(json).map_err(|e| e.to_string())?;
        validate(&config)?;
        Ok(config)
    }

    #[test]
    fn parse_size_variants() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("500MB").unwrap(), 500 << 20);
        assert_eq!(parse_size("10GB").unwrap(), 10 << 30);
        assert_eq!(parse_size("10 GiB").unwrap(), 10 << 30);
        assert_eq!(parse_size("1tb").unwrap(), 1 << 40);
        assert_eq!(parse_size("1.5GB").unwrap(), (1.5 * (1u64 << 30) as f64) as u64);
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
        let quotas = resolve_quotas(&config).unwrap();
        assert_eq!(quotas["a"], 1 << 20);
        assert_eq!(quotas["b"], 1 << 20);
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
        let quotas = resolve_quotas(&config).unwrap();
        assert_eq!(quotas["a"], 30u64 << 30);
        assert_eq!(quotas["b"], 50u64 << 30);
        assert_eq!(quotas["c"], 50u64 << 30);
    }

    #[test]
    fn missing_quota_and_total_is_error() {
        let err = parse_config(
            r#"{"users": [{"name": "a", "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}]}"#,
        )
        .unwrap_err();
        assert!(err.contains("no quota"), "{err}");
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
        let quotas = resolve_quotas(&config).unwrap();
        assert_eq!(quotas["a"], 1 << 20);
        assert_eq!(quotas["b"], 1 << 20);
        assert_eq!(config.users[0].rules[0].tag.as_deref(), Some("web"));
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
