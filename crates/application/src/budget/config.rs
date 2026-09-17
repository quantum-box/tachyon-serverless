//! `[budget]` and the budget file (PLT-4643, docs/adr/0016).
//!
//! Budgets are **configuration**: the control plane reads `[budget]` (and,
//! when `file` is set, re-reads that file at every publication) and delivers
//! one [`TenantBudget`] per known tenant through the configuration cache,
//! generation-stamped and valid for the auth lease. Data planes enforce what
//! was delivered, never their own copy of the file.
//!
//! Two separate settings per scope (tenant, and optionally function):
//! - `soft_limit_micros` + `alert_thresholds_percent`: alerts only (a log
//!   line, a metric and a row in `GET /v1/budget`), never a refusal;
//! - `hard_limit_micros`: new work whose maximum charge does not fit is
//!   refused (`budget_exhausted`). Absent: nothing is ever stopped.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use tachyon_serverless_domain::{FunctionId, TenantId};

/// The only period the prototype supports.
pub const PERIOD_CALENDAR_MONTH_UTC: &str = "calendar_month_utc";

/// Alert and stop settings of one scope, in integer micro-units of the price
/// table's currency (the same units as `GET /v1/usage`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetLimits {
    /// Base of the alert thresholds. Alerts only; never refuses anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_limit_micros: Option<u64>,
    /// Percentages of `soft_limit_micros` at which an alert fires once per
    /// period (e.g. `[50, 80, 100]`).
    #[serde(default)]
    pub alert_thresholds_percent: Vec<u32>,
    /// Stop: new work whose maximum charge would take the committed amount
    /// (reserved + settled + unmetered holds) above this is refused. `None`:
    /// never stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_limit_micros: Option<u64>,
}

impl BudgetLimits {
    pub fn validate(&self, what: &str) -> Result<(), String> {
        if !self.alert_thresholds_percent.is_empty() && self.soft_limit_micros.is_none() {
            return Err(format!(
                "{what}: alert_thresholds_percent needs soft_limit_micros (alerts and the hard \
                 limit are separate settings)"
            ));
        }
        let mut previous = 0;
        for &p in &self.alert_thresholds_percent {
            if p == 0 || p > 1000 {
                return Err(format!(
                    "{what}: alert thresholds must be within 1..=1000 percent, got {p}"
                ));
            }
            if p <= previous {
                return Err(format!(
                    "{what}: alert_thresholds_percent must be strictly increasing"
                ));
            }
            previous = p;
        }
        for v in [self.soft_limit_micros, self.hard_limit_micros]
            .into_iter()
            .flatten()
        {
            if v > i64::MAX as u64 {
                return Err(format!("{what}: limits must be at most {}", i64::MAX));
            }
        }
        Ok(())
    }
}

/// One function's budget inside its tenant's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionBudgetConfig {
    pub function_id: FunctionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_limit_micros: Option<u64>,
    #[serde(default)]
    pub alert_thresholds_percent: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_limit_micros: Option<u64>,
}

impl FunctionBudgetConfig {
    pub fn limits(&self) -> BudgetLimits {
        BudgetLimits {
            soft_limit_micros: self.soft_limit_micros,
            alert_thresholds_percent: self.alert_thresholds_percent.clone(),
            hard_limit_micros: self.hard_limit_micros,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantBudgetConfig {
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub soft_limit_micros: Option<u64>,
    #[serde(default)]
    pub alert_thresholds_percent: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_limit_micros: Option<u64>,
    #[serde(default)]
    pub functions: Vec<FunctionBudgetConfig>,
}

impl TenantBudgetConfig {
    pub fn limits(&self) -> BudgetLimits {
        BudgetLimits {
            soft_limit_micros: self.soft_limit_micros,
            alert_thresholds_percent: self.alert_thresholds_percent.clone(),
            hard_limit_micros: self.hard_limit_micros,
        }
    }
}

/// The budgets themselves: `[budget]` inline, or the file `[budget] file`
/// names (same keys at its top level). The file replaces the inline ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetBook {
    #[serde(default)]
    pub tenants: Vec<TenantBudgetConfig>,
    /// Budget of every known tenant without an entry. Without it such a
    /// tenant has **no** budget entry and its invocations are refused
    /// (`Host.BudgetUnknown`, fail closed).
    #[serde(default)]
    pub default_tenant: Option<BudgetLimits>,
}

impl BudgetBook {
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let book: Self = toml::from_str(text).map_err(|e| format!("budget file: {e}"))?;
        book.validate()?;
        Ok(book)
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read budget file {}: {e}", path.display()))?;
        Self::from_toml(&text)
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        for t in &self.tenants {
            if !seen.insert(t.tenant_id.clone()) {
                return Err(format!("budget: tenant {} is listed twice", t.tenant_id));
            }
            t.limits()
                .validate(&format!("budget tenant {}", t.tenant_id))?;
            let mut fns = std::collections::BTreeSet::new();
            for f in &t.functions {
                if !fns.insert(f.function_id.clone()) {
                    return Err(format!(
                        "budget: function {} of tenant {} is listed twice",
                        f.function_id, t.tenant_id
                    ));
                }
                f.limits()
                    .validate(&format!("budget function {}", f.function_id))?;
            }
        }
        if let Some(d) = &self.default_tenant {
            d.validate("budget default_tenant")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BudgetConfig {
    /// Enforce budgets on this gateway (reservation at admission, fail closed
    /// when the budget is unknown). A control plane with budgets configured
    /// publishes them either way.
    pub enabled: bool,
    /// `calendar_month_utc` (the only period of the prototype).
    pub period: String,
    /// A budget file re-read at every publication (live limit changes).
    pub file: Option<PathBuf>,
    #[serde(default)]
    pub tenants: Vec<TenantBudgetConfig>,
    pub default_tenant: Option<BudgetLimits>,
    /// Added to the billable-milliseconds bound of every reservation
    /// (scheduling noise, per-segment rounding up).
    pub reservation_slack_ms: u64,
    /// A reservation whose run never reported back (a crash) expires this
    /// long after its run deadline and keeps its maximum as an unmetered hold.
    pub expiry_grace_seconds: u64,
    /// New invocations are refused (`Host.BudgetUnknown`, collector stalled)
    /// once a finished run has waited this long for its usage to be
    /// collected and settled.
    pub max_unsettled_age_seconds: u64,
    /// Reservations settled per pass.
    pub settle_batch: usize,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            period: PERIOD_CALENDAR_MONTH_UTC.into(),
            file: None,
            tenants: Vec::new(),
            default_tenant: None,
            reservation_slack_ms: 250,
            expiry_grace_seconds: 30,
            max_unsettled_age_seconds: 30,
            settle_batch: 500,
        }
    }
}

impl BudgetConfig {
    /// Whether this gateway has budgets to publish.
    pub fn publishes(&self) -> bool {
        self.enabled
            || self.file.is_some()
            || !self.tenants.is_empty()
            || self.default_tenant.is_some()
    }

    pub fn inline_book(&self) -> BudgetBook {
        BudgetBook {
            tenants: self.tenants.clone(),
            default_tenant: self.default_tenant.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.period != PERIOD_CALENDAR_MONTH_UTC {
            return Err(format!(
                "[budget] period must be \"{PERIOD_CALENDAR_MONTH_UTC}\", got \"{}\"",
                self.period
            ));
        }
        if self.file.is_some() && (!self.tenants.is_empty() || self.default_tenant.is_some()) {
            return Err(
                "[budget] set either file or inline tenants / default_tenant, not both".into(),
            );
        }
        self.inline_book().validate()?;
        if self.max_unsettled_age_seconds == 0 {
            return Err("[budget] max_unsettled_age_seconds must be >= 1".into());
        }
        if self.settle_batch == 0 {
            return Err("[budget] settle_batch must be >= 1".into());
        }
        Ok(())
    }

    pub fn absolutize(&mut self, base: &Path) {
        if let Some(f) = self.file.as_mut()
            && f.is_relative()
        {
            *f = base.join(&*f);
        }
    }
}

/// What the control plane delivers for one tenant (`ConfigValue::Budget`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantBudget {
    pub tenant_id: TenantId,
    pub period: String,
    /// Currency and price table the amounts are in. A data plane rating with
    /// another table refuses (`Host.BudgetUnknown`): the units would differ.
    pub currency: String,
    pub price_table_version: String,
    pub limits: BudgetLimits,
    #[serde(default)]
    pub functions: Vec<FunctionBudget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionBudget {
    pub function_id: FunctionId,
    pub limits: BudgetLimits,
}

impl TenantBudget {
    pub fn function(&self, id: &FunctionId) -> Option<&BudgetLimits> {
        self.functions
            .iter()
            .find(|f| &f.function_id == id)
            .map(|f| &f.limits)
    }
}

impl BudgetBook {
    /// The delivered budget of `tenant`, if it has one.
    pub fn budget_for(
        &self,
        tenant: &TenantId,
        period: &str,
        currency: &str,
        price_table_version: &str,
    ) -> Option<TenantBudget> {
        let (limits, functions) = match self.tenants.iter().find(|t| &t.tenant_id == tenant) {
            Some(t) => (
                t.limits(),
                t.functions
                    .iter()
                    .map(|f| FunctionBudget {
                        function_id: f.function_id.clone(),
                        limits: f.limits(),
                    })
                    .collect(),
            ),
            None => (self.default_tenant.clone()?, Vec::new()),
        };
        Some(TenantBudget {
            tenant_id: tenant.clone(),
            period: period.to_string(),
            currency: currency.to_string(),
            price_table_version: price_table_version.to_string(),
            limits,
            functions,
        })
    }
}
