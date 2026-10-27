use serde::{Deserialize, Serialize};

use crate::config::{Provider, RemoteConfig, parse_size};
use crate::volcengine;

const EFFECTIVE: &str = "Effective";
const USED_UP: &str = "UsedUp";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Package {
    pub id: String,
    pub name: String,
    pub region: String,
    pub total: u64,
    pub available: u64,
    pub status: String,
    pub expiry: String,
}

impl Package {
    pub fn effective(&self) -> bool {
        self.status == EFFECTIVE
    }

    pub fn used_up(&self) -> bool {
        self.status == USED_UP
    }
}

pub fn amount(value: &str, unit: &str) -> Result<u64, String> {
    parse_size(&format!("{}{}", value.trim(), unit.trim()))
}

pub struct Fetched {
    pub packages: Vec<Package>,
    pub ignored: Vec<Package>,
}

pub async fn fetch(remote: &RemoteConfig) -> Result<Fetched, String> {
    let all = match remote.provider {
        Provider::Volcengine => {
            volcengine::list_traffic_packages(&remote.access_key, &remote.secret_key).await?
        }
    };
    let (packages, ignored) = all
        .into_iter()
        .partition(|package| remote.regions.contains(&package.region));
    Ok(Fetched { packages, ignored })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts_follow_their_unit() {
        assert_eq!(amount("1024", "GB").unwrap(), 1 << 40);
        assert_eq!(amount("1", "TB").unwrap(), 1 << 40);
        assert_eq!(amount(" 0.5 ", "GB").unwrap(), 1 << 29);
        assert!(amount("1", "PB").is_err());
        assert!(amount("", "GB").is_err());
    }
}
