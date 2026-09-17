//! `[usage]` (PLT-4642, docs/adr/0012).

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::config::Profile;

use super::journal::JournalLimits;

/// What happens when the usage journal is full or unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JournalFullPolicy {
    /// Refuse new invocations (`503 usage_journal_full`). Fail closed. The
    /// only policy allowed outside `profile = "dev"`.
    #[default]
    Refuse,
    /// Keep accepting. Events the journal cannot take are counted as
    /// unjournaled (unmetered) and never estimated. `profile = "dev"` only.
    AcceptUnmetered,
}

impl JournalFullPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::AcceptUnmetered => "accept_unmetered",
        }
    }
}

/// `[usage.billing]`. Hard-disabled in the prototype: `enabled = true` is a
/// configuration error. Nothing in this repository sends an invoice or
/// charges anyone.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct BillingConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// Whether billing can ever be on in this build. It cannot.
pub const BILLING_ENABLED: bool = false;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UsageConfig {
    /// Hard bound of pending (not yet collected) journal events.
    pub journal_max_events: u64,
    /// Hard bound of pending journal bytes (event bodies).
    pub journal_max_bytes: u64,
    /// New invocations are refused once this few events are left under
    /// `journal_max_events`, so admitted work still has room for its events.
    pub admission_headroom_events: u64,
    /// The same for bytes.
    pub admission_headroom_bytes: u64,
    pub on_journal_full: JournalFullPolicy,
    /// How often the collector delivers the journal to the ledger.
    pub collect_interval_ms: u64,
    /// Events per ledger transaction.
    pub collect_batch: usize,
    /// A price table file (TOML). The built-in provisional table otherwise.
    pub price_table: Option<PathBuf>,
    pub billing: BillingConfig,
}

impl Default for UsageConfig {
    fn default() -> Self {
        Self {
            journal_max_events: 100_000,
            journal_max_bytes: 64 * 1024 * 1024,
            admission_headroom_events: 1_000,
            admission_headroom_bytes: 1024 * 1024,
            on_journal_full: JournalFullPolicy::Refuse,
            collect_interval_ms: 1_000,
            collect_batch: 500,
            price_table: None,
            billing: BillingConfig::default(),
        }
    }
}

impl UsageConfig {
    pub fn limits(&self) -> JournalLimits {
        JournalLimits {
            max_events: self.journal_max_events,
            max_bytes: self.journal_max_bytes,
            admission_headroom_events: self.admission_headroom_events,
            admission_headroom_bytes: self.admission_headroom_bytes,
        }
    }

    pub fn collect_interval(&self) -> Duration {
        Duration::from_millis(self.collect_interval_ms)
    }

    pub fn validate(&self, profile: Profile) -> Result<(), String> {
        if self.billing.enabled {
            return Err(
                "[usage.billing] enabled = true is not supported: billing is hard-disabled in \
                 this prototype (usage is metered and rated provisionally, never invoiced)"
                    .into(),
            );
        }
        if self.on_journal_full == JournalFullPolicy::AcceptUnmetered && profile != Profile::Dev {
            return Err(
                "[usage] on_journal_full = \"accept_unmetered\" is only allowed with profile = \
                 \"dev\": production refuses invocations it cannot meter"
                    .into(),
            );
        }
        if self.journal_max_events == 0 || self.journal_max_bytes == 0 {
            return Err("[usage] journal_max_events and journal_max_bytes must be >= 1".into());
        }
        if self.admission_headroom_events >= self.journal_max_events
            || self.admission_headroom_bytes >= self.journal_max_bytes
        {
            return Err(
                "[usage] admission_headroom_events / _bytes must be below journal_max_events / \
                 _bytes"
                    .into(),
            );
        }
        if self.collect_interval_ms < 10 {
            return Err("[usage] collect_interval_ms must be >= 10".into());
        }
        if self.collect_batch == 0 {
            return Err("[usage] collect_batch must be >= 1".into());
        }
        Ok(())
    }
}
