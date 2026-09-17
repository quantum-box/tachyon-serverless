//! The control plane's budget publication (PLT-4643, docs/adr/0016 §1).
//!
//! Budgets come from `[budget]` inline, or from `[budget] file`, which is
//! re-read whenever its size or modification time changes (checked at every
//! publication and by the in-process source's change marker). A file that
//! becomes unreadable or invalid keeps the last valid budgets published and
//! reports the error (`/readyz` `budget.publication`); it never publishes a
//! half-parsed file. A tenant without an entry (and no `default_tenant`) gets
//! no budget entry at all, which a data plane enforcing budgets refuses.

use std::path::PathBuf;
use std::time::SystemTime;

use parking_lot::Mutex;
use serde::Serialize;

use tachyon_serverless_domain::TenantId;

use super::config::{BudgetBook, BudgetConfig, TenantBudget};
use crate::usage::PriceTable;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PublicationStatus {
    pub file: Option<String>,
    /// Successful (re)loads of the file.
    pub loads: u64,
    pub last_error: Option<String>,
    pub tenants: usize,
    pub has_default: bool,
}

struct State {
    signature: Option<(SystemTime, u64)>,
    book: BudgetBook,
    status: PublicationStatus,
}

pub struct BudgetPublisher {
    file: Option<PathBuf>,
    period: String,
    currency: String,
    price_table_version: String,
    state: Mutex<State>,
}

impl std::fmt::Debug for BudgetPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetPublisher")
            .field("file", &self.file)
            .finish_non_exhaustive()
    }
}

fn signature(path: &std::path::Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

impl BudgetPublisher {
    /// A file that cannot be loaded at startup is a configuration error.
    pub fn new(config: &BudgetConfig, table: &PriceTable) -> Result<Self, String> {
        let (book, signature, loads) = match &config.file {
            Some(path) => (BudgetBook::load(path)?, signature(path), 1),
            None => (config.inline_book(), None, 0),
        };
        let status = PublicationStatus {
            file: config.file.as_ref().map(|p| p.display().to_string()),
            loads,
            last_error: None,
            tenants: book.tenants.len(),
            has_default: book.default_tenant.is_some(),
        };
        Ok(Self {
            file: config.file.clone(),
            period: config.period.clone(),
            currency: table.currency.clone(),
            price_table_version: table.version.clone(),
            state: Mutex::new(State {
                signature,
                book,
                status,
            }),
        })
    }

    /// Re-read the file when it changed. Keeps the last valid book on error.
    fn refresh(&self) -> BudgetBook {
        let mut st = self.state.lock();
        if let Some(path) = &self.file {
            let sig = signature(path);
            if sig != st.signature {
                match BudgetBook::load(path) {
                    Ok(book) => {
                        tracing::info!(
                            file = %path.display(),
                            tenants = book.tenants.len(),
                            "budget file reloaded"
                        );
                        st.status.loads += 1;
                        st.status.last_error = None;
                        st.status.tenants = book.tenants.len();
                        st.status.has_default = book.default_tenant.is_some();
                        st.book = book;
                        st.signature = sig;
                    }
                    Err(e) => {
                        if st.status.last_error.as_deref() != Some(e.as_str()) {
                            tracing::error!(
                                error = %e,
                                "budget file invalid; the last valid budgets stay published"
                            );
                        }
                        st.status.last_error = Some(e);
                    }
                }
            }
        }
        st.book.clone()
    }

    /// Moves whenever the file's size or modification time changes.
    pub fn marker(&self) -> u64 {
        let Some(path) = &self.file else {
            return 0;
        };
        match signature(path) {
            Some((t, len)) => {
                let nanos = t
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                nanos ^ len.rotate_left(40)
            }
            None => u64::MAX,
        }
    }

    /// The budget entries of `tenants`.
    pub fn budgets(&self, tenants: &[TenantId]) -> Vec<TenantBudget> {
        let book = self.refresh();
        tenants
            .iter()
            .filter_map(|t| {
                book.budget_for(t, &self.period, &self.currency, &self.price_table_version)
            })
            .collect()
    }

    pub fn status(&self) -> PublicationStatus {
        self.state.lock().status.clone()
    }
}
