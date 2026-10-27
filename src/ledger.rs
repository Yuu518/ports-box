use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::config::format_size;
use crate::provider::Package;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "user", rename_all = "lowercase")]
pub enum Credit {
    Migration,
    Split,
    User(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageRecord {
    #[serde(flatten)]
    pub package: Package,
    #[serde(default)]
    pub credit: Option<Credit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    Migration,
    Credit,
    Expiry,
    Transfer,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryKind::Migration => "migration",
            EntryKind::Credit => "credit",
            EntryKind::Expiry => "expiry",
            EntryKind::Transfer => "transfer",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        [
            EntryKind::Migration,
            EntryKind::Credit,
            EntryKind::Expiry,
            EntryKind::Transfer,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub time: i64,
    pub kind: EntryKind,
    pub user: String,
    pub amount: i64,
    pub package: Option<String>,
    pub note: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    #[serde(default)]
    pub migrated: bool,
    #[serde(default)]
    pub remaining: u64,
    #[serde(default)]
    pub synced_at: i64,
    #[serde(default)]
    pub consumed: u64,
    #[serde(default)]
    pub metered: u64,
    #[serde(default)]
    pub balances: BTreeMap<String, i64>,
    #[serde(default)]
    pub baselines: BTreeMap<String, u64>,
    #[serde(default)]
    pub packages: BTreeMap<String, PackageRecord>,
}

pub struct Member {
    pub name: String,
    pub used: u64,
    pub offline_remaining: Option<u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Assignments {
    pub split: HashSet<String>,
    pub owners: HashMap<String, String>,
}

fn split_by(amount: u64, weights: &[u64]) -> Vec<u64> {
    let total: u128 = weights.iter().map(|&w| w as u128).sum();
    if total == 0 {
        return vec![0; weights.len()];
    }
    let mut shares: Vec<u64> = Vec::with_capacity(weights.len());
    let mut remainders: Vec<(u128, usize)> = Vec::with_capacity(weights.len());
    for (i, &weight) in weights.iter().enumerate() {
        let exact = amount as u128 * weight as u128;
        shares.push((exact / total) as u64);
        remainders.push((exact % total, i));
    }
    let mut left = amount - shares.iter().sum::<u64>();
    remainders.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    for (_, i) in remainders {
        if left == 0 {
            break;
        }
        shares[i] += 1;
        left -= 1;
    }
    shares
}

fn split_evenly(amount: u64, count: usize) -> Vec<u64> {
    split_by(amount, &vec![1; count])
}

fn signed(amount: u64) -> i64 {
    i64::try_from(amount).unwrap_or(i64::MAX)
}

impl Book {
    pub fn limit(&self, name: &str) -> Option<u64> {
        let baseline = *self.baselines.get(name)?;
        let balance = self.balances.get(name).copied().unwrap_or(0).max(0);
        Some(baseline.saturating_add(balance as u64))
    }

    pub fn limits(&self) -> HashMap<String, u64> {
        self.baselines
            .keys()
            .filter_map(|name| Some((name.clone(), self.limit(name)?)))
            .collect()
    }

    pub fn unallocated(&self) -> i64 {
        signed(self.remaining).saturating_sub(self.balances.values().sum())
    }

    fn join(&mut self, members: &[Member]) {
        for member in members {
            self.baselines
                .entry(member.name.clone())
                .or_insert(member.used);
            self.balances.entry(member.name.clone()).or_insert(0);
        }
    }

    fn migrate(&mut self, packages: Vec<Package>, members: &[Member], now: i64) -> Vec<Entry> {
        let weights: Vec<u64> = members
            .iter()
            .map(|member| member.offline_remaining.unwrap_or(0))
            .collect();
        let shares = if weights.iter().any(|&w| w > 0) {
            split_by(self.remaining, &weights)
        } else {
            split_evenly(self.remaining, members.len())
        };
        let mut entries = Vec::new();
        for (member, share) in members.iter().zip(shares) {
            self.balances.insert(member.name.clone(), signed(share));
            self.baselines.insert(member.name.clone(), member.used);
            entries.push(Entry {
                time: now,
                kind: EntryKind::Migration,
                user: member.name.clone(),
                amount: signed(share),
                package: None,
                note: String::new(),
            });
        }
        for package in packages {
            self.packages.insert(
                package.id.clone(),
                PackageRecord {
                    package,
                    credit: Some(Credit::Migration),
                },
            );
        }
        self.migrated = true;
        entries
    }

    pub fn reconcile(
        &mut self,
        packages: Vec<Package>,
        members: &[Member],
        assignments: &Assignments,
        now: i64,
    ) -> Vec<Entry> {
        let remaining: u64 = packages
            .iter()
            .filter(|package| package.effective())
            .map(|package| package.available)
            .sum();
        let previous = std::mem::replace(&mut self.remaining, remaining);
        self.synced_at = now;
        if !self.migrated {
            self.consumed = 0;
            self.metered = 0;
            return self.migrate(packages, members, now);
        }
        self.join(members);

        let fetched: HashMap<&str, &Package> = packages
            .iter()
            .map(|package| (package.id.as_str(), package))
            .collect();
        let mut gone = 0u64;
        let mut forfeited = Vec::new();
        for (id, record) in &mut self.packages {
            if !record.package.effective() {
                continue;
            }
            let current = fetched.get(id.as_str());
            if current.is_some_and(|package| package.effective()) {
                continue;
            }
            let leftover = if current.is_some_and(|package| package.used_up()) {
                0
            } else {
                record.package.available
            };
            gone += leftover;
            if record.credit.is_some() && leftover > 0 {
                forfeited.push((id.clone(), leftover));
            }
            if current.is_none() {
                record.package.status = "Missing".into();
            }
        }
        let mut added = 0u64;
        for package in packages {
            match self.packages.get_mut(&package.id) {
                Some(record) => {
                    if package.effective() && !record.package.effective() {
                        added += package.total;
                    }
                    record.package = package;
                }
                None => {
                    if package.effective() {
                        added += package.total;
                    }
                    self.packages.insert(
                        package.id.clone(),
                        PackageRecord {
                            package,
                            credit: None,
                        },
                    );
                }
            }
        }

        let mut entries = Vec::new();
        let names: Vec<&str> = members.iter().map(|member| member.name.as_str()).collect();
        for (id, record) in &mut self.packages {
            if record.credit.is_some() || !record.package.effective() {
                continue;
            }
            let total = record.package.total;
            if let Some(owner) = assignments.owners.get(id)
                && names.contains(&owner.as_str())
            {
                *self.balances.entry(owner.clone()).or_insert(0) += signed(total);
                record.credit = Some(Credit::User(owner.clone()));
                entries.push(Entry {
                    time: now,
                    kind: EntryKind::Credit,
                    user: owner.clone(),
                    amount: signed(total),
                    package: Some(id.clone()),
                    note: String::new(),
                });
            } else if assignments.split.contains(id) && !names.is_empty() {
                for (name, share) in names.iter().zip(split_evenly(total, names.len())) {
                    *self.balances.entry((*name).to_owned()).or_insert(0) += signed(share);
                    entries.push(Entry {
                        time: now,
                        kind: EntryKind::Credit,
                        user: (*name).to_owned(),
                        amount: signed(share),
                        package: Some(id.clone()),
                        note: String::new(),
                    });
                }
                record.credit = Some(Credit::Split);
            }
        }

        let consumed = (previous as i128 + added as i128 - gone as i128 - remaining as i128)
            .clamp(0, u64::MAX as i128) as u64;
        let usage: Vec<u64> = members
            .iter()
            .map(|member| member.used.saturating_sub(self.baselines[&member.name]))
            .collect();
        let metered = usage
            .iter()
            .fold(0u64, |sum, &used| sum.saturating_add(used));
        if consumed > 0 && metered > 0 {
            for (member, charge) in members.iter().zip(split_by(consumed, &usage)) {
                *self.balances.get_mut(&member.name).expect("member joined") -= signed(charge);
            }
        }
        for member in members {
            self.baselines.insert(member.name.clone(), member.used);
        }
        self.consumed = consumed;
        self.metered = metered;

        for (id, leftover) in forfeited {
            let holders: Vec<(String, u64)> = self
                .balances
                .iter()
                .filter(|(_, balance)| **balance > 0)
                .map(|(name, balance)| (name.clone(), *balance as u64))
                .collect();
            let weights: Vec<u64> = holders.iter().map(|(_, balance)| *balance).collect();
            for ((name, _), share) in holders.iter().zip(split_by(leftover, &weights)) {
                if share == 0 {
                    continue;
                }
                *self.balances.get_mut(name).expect("holder exists") -= signed(share);
                entries.push(Entry {
                    time: now,
                    kind: EntryKind::Expiry,
                    user: name.clone(),
                    amount: -signed(share),
                    package: Some(id.clone()),
                    note: String::new(),
                });
            }
        }
        entries
    }

    pub fn transfer(
        &mut self,
        members: &[Member],
        from: &str,
        to: &str,
        amount: u64,
        now: i64,
    ) -> Result<Vec<Entry>, String> {
        if !self.migrated {
            return Err("remote accounting has not synced yet".into());
        }
        if amount == 0 {
            return Err("transfer amount must be positive".into());
        }
        if from == to {
            return Err("cannot transfer to the same user".into());
        }
        let find = |name: &str| {
            members
                .iter()
                .find(|member| member.name == name)
                .ok_or_else(|| format!("unknown user {name:?}"))
        };
        let source = find(from)?;
        find(to)?;
        self.join(members);
        let spent = source.used.saturating_sub(self.baselines[from]);
        let available = self.balances[from].saturating_sub(signed(spent));
        if signed(amount) > available {
            return Err(format!(
                "{from} has only {} left",
                format_size(available.max(0) as u64)
            ));
        }
        *self.balances.get_mut(from).expect("joined") -= signed(amount);
        *self.balances.get_mut(to).expect("joined") += signed(amount);
        Ok(vec![
            Entry {
                time: now,
                kind: EntryKind::Transfer,
                user: from.to_owned(),
                amount: -signed(amount),
                package: None,
                note: to.to_owned(),
            },
            Entry {
                time: now,
                kind: EntryKind::Transfer,
                user: to.to_owned(),
                amount: signed(amount),
                package: None,
                note: from.to_owned(),
            },
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(id: &str, total: u64, available: u64, status: &str) -> Package {
        Package {
            id: id.into(),
            name: id.into(),
            region: "cn-shanghai".into(),
            total,
            available,
            status: status.into(),
            expiry: "2027-06-28T15:59:59Z".into(),
        }
    }

    fn member(name: &str, used: u64, offline_remaining: Option<u64>) -> Member {
        Member {
            name: name.into(),
            used,
            offline_remaining,
        }
    }

    fn migrated(balances: &[(&str, u64)], packages: Vec<Package>) -> Book {
        let mut book = Book::default();
        let members: Vec<Member> = balances
            .iter()
            .map(|(name, balance)| member(name, 0, Some(*balance)))
            .collect();
        book.reconcile(packages, &members, &Assignments::default(), 1);
        book
    }

    fn balance(book: &Book, name: &str) -> i64 {
        book.balances[name]
    }

    #[test]
    fn split_by_keeps_every_byte() {
        assert_eq!(split_by(10, &[1, 1, 1]), [4, 3, 3]);
        assert_eq!(split_by(10, &[0, 0]), [0, 0]);
        assert_eq!(split_by(7, &[3, 1]), [5, 2]);
        assert_eq!(
            split_by(u64::MAX, &[u64::MAX, u64::MAX])
                .iter()
                .sum::<u64>(),
            u64::MAX
        );
        assert!(split_by(5, &[]).is_empty());
    }

    #[test]
    fn migration_scales_offline_remaining_to_the_provider() {
        let mut book = Book::default();
        let members = [
            member("a", 300, Some(190)),
            member("b", 60, Some(410)),
            member("c", 5, None),
        ];
        let entries = book.reconcile(
            vec![
                package("p1", 1200, 1200, "Effective"),
                package("p2", 1200, 0, "UsedUp"),
            ],
            &members,
            &Assignments::default(),
            7,
        );
        assert!(book.migrated);
        assert_eq!(book.remaining, 1200);
        assert_eq!(balance(&book, "a"), 380);
        assert_eq!(balance(&book, "b"), 820);
        assert_eq!(balance(&book, "c"), 0);
        assert_eq!(book.limit("a"), Some(680));
        assert_eq!(book.unallocated(), 0);
        assert_eq!(entries.len(), 3);
        assert!(
            book.packages
                .values()
                .all(|record| record.credit == Some(Credit::Migration))
        );

        let mut book = Book::default();
        book.reconcile(
            vec![package("p1", 9, 9, "Effective")],
            &[member("a", 0, None), member("b", 0, None)],
            &Assignments::default(),
            7,
        );
        assert_eq!((balance(&book, "a"), balance(&book, "b")), (5, 4));
    }

    #[test]
    fn consumption_is_charged_by_local_usage() {
        let mut book = migrated(
            &[("a", 500), ("b", 500)],
            vec![package("p", 1000, 1000, "Effective")],
        );
        let members = [member("a", 30, None), member("b", 10, None)];

        book.reconcile(
            vec![package("p", 1000, 920, "Effective")],
            &members,
            &Assignments::default(),
            2,
        );

        assert_eq!((book.consumed, book.metered), (80, 40));
        assert_eq!(balance(&book, "a"), 440);
        assert_eq!(balance(&book, "b"), 480);
        assert_eq!(book.limit("a"), Some(470));
        assert_eq!(book.unallocated(), 0);

        book.reconcile(
            vec![package("p", 1000, 900, "Effective")],
            &members,
            &Assignments::default(),
            3,
        );
        assert_eq!((book.consumed, book.metered), (20, 0));
        assert_eq!(book.unallocated(), -20);
    }

    #[test]
    fn listed_packages_are_credited_once() {
        let mut book = migrated(
            &[("a", 10), ("b", 10)],
            vec![package("old", 20, 20, "Effective")],
        );
        let members = [member("a", 0, None), member("b", 0, None)];
        let assignments = Assignments {
            split: HashSet::from(["s".to_owned()]),
            owners: HashMap::from([("o".to_owned(), "b".to_owned())]),
        };
        let packages = vec![
            package("old", 20, 20, "Effective"),
            package("s", 101, 101, "Effective"),
            package("o", 50, 50, "Effective"),
            package("x", 70, 70, "Effective"),
        ];

        let entries = book.reconcile(packages.clone(), &members, &assignments, 2);

        assert_eq!(entries.len(), 3);
        assert_eq!(balance(&book, "a"), 10 + 51);
        assert_eq!(balance(&book, "b"), 10 + 50 + 50);
        assert_eq!(book.consumed, 0);
        assert_eq!(book.unallocated(), 70);
        assert_eq!(book.packages["s"].credit, Some(Credit::Split));
        assert_eq!(book.packages["o"].credit, Some(Credit::User("b".into())));
        assert_eq!(book.packages["x"].credit, None);

        assert!(
            book.reconcile(packages.clone(), &members, &assignments, 3)
                .is_empty()
        );

        let assignments = Assignments {
            split: HashSet::from(["s".to_owned(), "x".to_owned()]),
            ..assignments
        };
        book.reconcile(packages, &members, &assignments, 4);
        assert_eq!(balance(&book, "a"), 61 + 35);
        assert_eq!(book.unallocated(), 0);
        assert_eq!(book.consumed, 0);
    }

    #[test]
    fn owner_must_be_a_current_member() {
        let mut book = migrated(&[("a", 0)], vec![]);
        let assignments = Assignments {
            owners: HashMap::from([("o".to_owned(), "ghost".to_owned())]),
            ..Default::default()
        };
        book.reconcile(
            vec![package("o", 50, 50, "Effective")],
            &[member("a", 0, None)],
            &assignments,
            2,
        );
        assert_eq!(book.packages["o"].credit, None);
        assert!(!book.balances.contains_key("ghost"));
    }

    #[test]
    fn expired_leftover_is_forfeited_by_balance() {
        let mut book = migrated(
            &[("a", 300), ("b", 100)],
            vec![
                package("e", 1000, 200, "Effective"),
                package("u", 1000, 200, "Effective"),
            ],
        );
        let members = [member("a", 0, None), member("b", 0, None)];

        let entries = book.reconcile(
            vec![
                package("e", 1000, 200, "Expired"),
                package("u", 1000, 0, "UsedUp"),
            ],
            &members,
            &Assignments::default(),
            2,
        );

        assert_eq!(book.consumed, 200);
        assert_eq!(book.metered, 0);
        assert_eq!(balance(&book, "a"), 150);
        assert_eq!(balance(&book, "b"), 50);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|entry| entry.kind == EntryKind::Expiry));
        assert_eq!(book.remaining, 0);
        assert_eq!(book.unallocated(), -200);
    }

    #[test]
    fn vanished_package_counts_as_gone_once() {
        let mut book = migrated(&[("a", 100)], vec![package("p", 100, 100, "Effective")]);
        let members = [member("a", 0, None)];
        book.reconcile(vec![], &members, &Assignments::default(), 2);
        assert_eq!(balance(&book, "a"), 0);
        assert_eq!(book.packages["p"].package.status, "Missing");
        book.reconcile(vec![], &members, &Assignments::default(), 3);
        assert_eq!(balance(&book, "a"), 0);
        assert_eq!(book.consumed, 0);
    }

    #[test]
    fn new_member_starts_empty() {
        let mut book = migrated(&[("a", 100)], vec![package("p", 100, 100, "Effective")]);
        let members = [member("a", 0, None), member("n", 42, None)];
        book.reconcile(
            vec![package("p", 100, 100, "Effective")],
            &members,
            &Assignments::default(),
            2,
        );
        assert_eq!(balance(&book, "n"), 0);
        assert_eq!(book.limit("n"), Some(42));
        assert_eq!(book.limit("missing"), None);
    }

    #[test]
    fn transfers_move_spendable_balance() {
        let mut book = migrated(
            &[("a", 100), ("b", 0)],
            vec![package("p", 100, 100, "Effective")],
        );
        let members = [
            member("a", 30, None),
            member("b", 0, None),
            member("c", 9, None),
        ];

        let entries = book.transfer(&members, "a", "c", 50, 5).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].amount, -50);
        assert_eq!(balance(&book, "a"), 50);
        assert_eq!(balance(&book, "c"), 50);
        assert_eq!(book.limit("c"), Some(59));
        for (from, to, amount, want) in [
            ("a", "b", 21, "only"),
            ("a", "a", 1, "same user"),
            ("a", "z", 1, "unknown user"),
            ("a", "b", 0, "positive"),
        ] {
            let err = book.transfer(&members, from, to, amount, 6).unwrap_err();
            assert!(err.contains(want), "{err}");
        }
        book.transfer(&members, "a", "b", 20, 6).unwrap();

        let err = Book::default()
            .transfer(&members, "a", "b", 1, 1)
            .unwrap_err();
        assert!(err.contains("not synced"), "{err}");
    }

    #[test]
    fn book_round_trips_through_json() {
        let mut book = migrated(&[("a", 100)], vec![package("p", 100, 100, "Effective")]);
        book.packages.get_mut("p").unwrap().credit = Some(Credit::User("a".into()));
        let json = serde_json::to_string(&book).unwrap();
        assert_eq!(serde_json::from_str::<Book>(&json).unwrap(), book);
        assert_eq!(EntryKind::parse("expiry"), Some(EntryKind::Expiry));
        assert_eq!(EntryKind::parse("nope"), None);
    }
}
