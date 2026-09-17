//! Resource vectors and node capacity.

use serde::Serialize;

use tachyon_serverless_domain::ResourceProfile;

/// What one environment reserves on the node, or what the node has.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_mib: u64,
    pub ephemeral_storage_mib: u64,
}

impl Resources {
    pub const ZERO: Self = Self {
        cpu_millis: 0,
        memory_mib: 0,
        ephemeral_storage_mib: 0,
    };

    /// A revision's resources plus the node's per-environment overhead.
    pub fn for_environment(profile: &ResourceProfile, overhead: Resources) -> Self {
        Self {
            cpu_millis: u64::from(profile.cpu_millis),
            memory_mib: u64::from(profile.memory_mib),
            ephemeral_storage_mib: u64::from(profile.ephemeral_storage_mib),
        }
        .plus(overhead)
    }

    pub fn plus(self, other: Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_add(other.cpu_millis),
            memory_mib: self.memory_mib.saturating_add(other.memory_mib),
            ephemeral_storage_mib: self
                .ephemeral_storage_mib
                .saturating_add(other.ephemeral_storage_mib),
        }
    }

    /// `self - other`. Panics in debug builds on underflow: the ledger never
    /// releases more than it reserved.
    pub fn minus(self, other: Self) -> Self {
        debug_assert!(
            self.cpu_millis >= other.cpu_millis
                && self.memory_mib >= other.memory_mib
                && self.ephemeral_storage_mib >= other.ephemeral_storage_mib,
            "releasing {other:?} from {self:?}"
        );
        Self {
            cpu_millis: self.cpu_millis.saturating_sub(other.cpu_millis),
            memory_mib: self.memory_mib.saturating_sub(other.memory_mib),
            ephemeral_storage_mib: self
                .ephemeral_storage_mib
                .saturating_sub(other.ephemeral_storage_mib),
        }
    }
}

/// The node's capacity. `None` in a dimension means unbounded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct NodeCapacity {
    pub cpu_millis: Option<u64>,
    pub memory_mib: Option<u64>,
    pub ephemeral_storage_mib: Option<u64>,
}

impl NodeCapacity {
    /// Whether `used` fits.
    pub fn admits(&self, used: Resources) -> bool {
        let ok = |cap: Option<u64>, v: u64| cap.is_none_or(|c| v <= c);
        ok(self.cpu_millis, used.cpu_millis)
            && ok(self.memory_mib, used.memory_mib)
            && ok(self.ephemeral_storage_mib, used.ephemeral_storage_mib)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_is_part_of_every_reservation() {
        let r = Resources::for_environment(
            &ResourceProfile {
                memory_mib: 256,
                cpu_millis: 500,
                ephemeral_storage_mib: 128,
            },
            Resources {
                cpu_millis: 50,
                memory_mib: 24,
                ephemeral_storage_mib: 4,
            },
        );
        assert_eq!(
            r,
            Resources {
                cpu_millis: 550,
                memory_mib: 280,
                ephemeral_storage_mib: 132
            }
        );
        let cap = NodeCapacity {
            cpu_millis: None,
            memory_mib: Some(560),
            ephemeral_storage_mib: None,
        };
        assert!(cap.admits(r.plus(r)));
        assert!(!cap.admits(r.plus(r).plus(Resources {
            memory_mib: 1,
            ..Resources::ZERO
        })));
    }
}
