//! Egress allowlist of a `restricted` revision and the address ranges no
//! egress profile may ever reach (PLT-4622, docs/adr/0005-egress-profiles.md).
//!
//! The model is IPv4 only on purpose: guests are given no IPv6 address or
//! route, and providers drop every IPv6 packet from a guest. A rule naming an
//! IPv6 network is therefore rejected at deploy time instead of being accepted
//! and silently never matching.

use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// Most allowlist rules one revision may carry.
pub const MAX_EGRESS_ALLOW_RULES: usize = 16;
/// Most destination ports one rule may list.
pub const MAX_EGRESS_ALLOW_PORTS: usize = 16;

/// Transport protocol of an allowlist rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EgressProtocol {
    #[default]
    Tcp,
    Udp,
}

impl EgressProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

/// One destination a `restricted` guest may open: an IPv4 network, a protocol
/// and a non-empty list of ports. Hostnames are not accepted (see the module
/// docs of the ADR): a name would have to be resolved by the host at
/// environment creation and could change under the guest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EgressAllowRule {
    /// IPv4 network in CIDR form (`203.0.113.7/32`). A bare address means `/32`.
    pub cidr: String,
    #[serde(default)]
    pub protocol: EgressProtocol,
    pub ports: Vec<u16>,
}

impl EgressAllowRule {
    /// Parsed network. Fails for anything [`Self::validate`] rejects.
    pub fn network(&self) -> Result<Ipv4Cidr, DomainError> {
        Ipv4Cidr::parse(&self.cidr)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        let net = self.network()?;
        if let Some(blocked) = BLOCKED_IPV4.iter().find(|b| b.cidr().overlaps(&net)) {
            return Err(DomainError::validation(
                "egress_allow.cidr",
                format!(
                    "`{}` overlaps {} ({}), which no egress profile may reach",
                    self.cidr, blocked.cidr, blocked.name
                ),
            ));
        }
        if self.ports.is_empty() || self.ports.len() > MAX_EGRESS_ALLOW_PORTS {
            return Err(DomainError::validation(
                "egress_allow.ports",
                format!("must list 1..={MAX_EGRESS_ALLOW_PORTS} ports"),
            ));
        }
        if self.ports.contains(&0) {
            return Err(DomainError::validation(
                "egress_allow.ports",
                "port 0 is not a destination port",
            ));
        }
        Ok(())
    }
}

/// An IPv4 network: address with the host bits cleared, and a prefix length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ipv4Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Ipv4Cidr {
    /// Parse `a.b.c.d/len` or `a.b.c.d` (`/32`). Host bits must be zero, so a
    /// typo such as `10.1.2.3/8` is an error rather than a wider network.
    pub fn parse(s: &str) -> Result<Self, DomainError> {
        let invalid =
            |reason: &str| DomainError::validation("egress_allow.cidr", format!("`{s}`: {reason}"));
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (
                a,
                p.parse::<u8>()
                    .ok()
                    .filter(|p| *p <= 32)
                    .ok_or_else(|| invalid("prefix length must be 0..=32"))?,
            ),
            None => (s, 32),
        };
        if addr.contains(':') {
            return Err(invalid(
                "IPv6 is not provided to guests; only IPv4 networks can be allowed",
            ));
        }
        let addr: Ipv4Addr = addr
            .parse()
            .map_err(|_| invalid("not an IPv4 address in CIDR form"))?;
        let cidr = Self::new(addr, prefix);
        if cidr.network != addr {
            return Err(invalid(&format!(
                "host bits are set (did you mean {}?)",
                cidr
            )));
        }
        Ok(cidr)
    }

    /// Network containing `addr` with `prefix` bits (host bits cleared).
    pub fn new(addr: Ipv4Addr, prefix: u8) -> Self {
        let prefix = prefix.min(32);
        Self {
            network: Ipv4Addr::from(u32::from(addr) & Self::mask_bits(prefix)),
            prefix,
        }
    }

    const fn mask_bits(prefix: u8) -> u32 {
        if prefix == 0 {
            0
        } else {
            u32::MAX << (32 - prefix as u32)
        }
    }

    pub fn network(&self) -> Ipv4Addr {
        self.network
    }

    pub fn prefix(&self) -> u8 {
        self.prefix
    }

    pub fn netmask(&self) -> Ipv4Addr {
        Ipv4Addr::from(Self::mask_bits(self.prefix))
    }

    /// Number of addresses in the network.
    pub fn size(&self) -> u64 {
        1u64 << (32 - self.prefix as u32)
    }

    pub fn contains(&self, addr: Ipv4Addr) -> bool {
        u32::from(addr) & Self::mask_bits(self.prefix) == u32::from(self.network)
    }

    /// True when `other` lies entirely inside `self`.
    pub fn contains_net(&self, other: &Ipv4Cidr) -> bool {
        other.prefix >= self.prefix && self.contains(other.network)
    }

    /// True when the two networks share at least one address.
    pub fn overlaps(&self, other: &Ipv4Cidr) -> bool {
        self.contains_net(other) || other.contains_net(self)
    }
}

impl std::fmt::Display for Ipv4Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// A named special-purpose range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockedRange {
    pub cidr: &'static str,
    pub name: &'static str,
}

impl BlockedRange {
    /// The IPv4 network of an entry of [`BLOCKED_IPV4`].
    pub fn cidr(&self) -> Ipv4Cidr {
        Ipv4Cidr::parse(self.cidr).expect("BLOCKED_IPV4 entries are valid")
    }
}

/// IPv4 destinations no guest may reach under any egress profile: the IANA
/// special-purpose registry entries that are not globally reachable unicast
/// (RFC 6890 and successors). They cover the management network and the node
/// (RFC1918, loopback, link-local and the `169.254.169.254` metadata address),
/// carrier-grade NAT, benchmarking and documentation ranges, multicast and
/// broadcast.
pub const BLOCKED_IPV4: &[BlockedRange] = &[
    BlockedRange {
        cidr: "0.0.0.0/8",
        name: "this network",
    },
    BlockedRange {
        cidr: "10.0.0.0/8",
        name: "RFC1918 private",
    },
    BlockedRange {
        cidr: "100.64.0.0/10",
        name: "CGNAT shared address space",
    },
    BlockedRange {
        cidr: "127.0.0.0/8",
        name: "loopback",
    },
    BlockedRange {
        cidr: "169.254.0.0/16",
        name: "link-local and cloud metadata",
    },
    BlockedRange {
        cidr: "172.16.0.0/12",
        name: "RFC1918 private",
    },
    BlockedRange {
        cidr: "192.0.0.0/24",
        name: "IETF protocol assignments",
    },
    BlockedRange {
        cidr: "192.0.2.0/24",
        name: "TEST-NET-1",
    },
    BlockedRange {
        cidr: "192.88.99.0/24",
        name: "6to4 relay anycast",
    },
    BlockedRange {
        cidr: "192.168.0.0/16",
        name: "RFC1918 private",
    },
    BlockedRange {
        cidr: "198.18.0.0/15",
        name: "benchmarking",
    },
    BlockedRange {
        cidr: "198.51.100.0/24",
        name: "TEST-NET-2",
    },
    BlockedRange {
        cidr: "203.0.113.0/24",
        name: "TEST-NET-3",
    },
    BlockedRange {
        cidr: "224.0.0.0/4",
        name: "multicast",
    },
    BlockedRange {
        cidr: "240.0.0.0/4",
        name: "reserved and limited broadcast",
    },
];

/// IPv6 counterparts, for providers that want to name them explicitly (for
/// example in a firewall set) on top of dropping IPv6 from guests entirely.
pub const BLOCKED_IPV6: &[BlockedRange] = &[
    BlockedRange {
        cidr: "::/128",
        name: "unspecified",
    },
    BlockedRange {
        cidr: "::1/128",
        name: "loopback",
    },
    BlockedRange {
        cidr: "::ffff:0:0/96",
        name: "IPv4-mapped",
    },
    BlockedRange {
        cidr: "64:ff9b::/96",
        name: "NAT64",
    },
    BlockedRange {
        cidr: "64:ff9b:1::/48",
        name: "local NAT64",
    },
    BlockedRange {
        cidr: "100::/64",
        name: "discard-only",
    },
    BlockedRange {
        cidr: "2001::/23",
        name: "IETF protocol assignments",
    },
    BlockedRange {
        cidr: "2001:db8::/32",
        name: "documentation",
    },
    BlockedRange {
        cidr: "2002::/16",
        name: "6to4",
    },
    BlockedRange {
        cidr: "fc00::/7",
        name: "unique local",
    },
    BlockedRange {
        cidr: "fe80::/10",
        name: "link-local",
    },
    BlockedRange {
        cidr: "fec0::/10",
        name: "site-local (deprecated)",
    },
    BlockedRange {
        cidr: "ff00::/8",
        name: "multicast",
    },
];

/// True when `addr` is not in any [`BLOCKED_IPV4`] range, i.e. it is a
/// globally reachable unicast address that `public-web` may open.
pub fn is_public_ipv4(addr: Ipv4Addr) -> bool {
    !BLOCKED_IPV4.iter().any(|b| b.cidr().contains(addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(cidr: &str, ports: &[u16]) -> EgressAllowRule {
        EgressAllowRule {
            cidr: cidr.into(),
            protocol: EgressProtocol::Tcp,
            ports: ports.to_vec(),
        }
    }

    #[test]
    fn cidrs_parse_strictly() {
        let c = Ipv4Cidr::parse("1.1.1.0/24").unwrap();
        assert_eq!(c.to_string(), "1.1.1.0/24");
        assert_eq!(c.netmask(), Ipv4Addr::new(255, 255, 255, 0));
        assert_eq!(c.size(), 256);
        assert!(c.contains(Ipv4Addr::new(1, 1, 1, 200)));
        assert!(!c.contains(Ipv4Addr::new(1, 1, 2, 0)));
        assert_eq!(Ipv4Cidr::parse("8.8.8.8").unwrap().prefix(), 32);
        for bad in [
            "1.1.1.1/33",
            "1.1.1.1/",
            "10.1.2.3/8",
            "::1/128",
            "example.com",
            "",
        ] {
            assert!(Ipv4Cidr::parse(bad).is_err(), "{bad}");
        }
        assert!(
            Ipv4Cidr::parse("0.0.0.0/0")
                .unwrap()
                .contains(Ipv4Addr::BROADCAST)
        );
    }

    #[test]
    fn overlap_is_symmetric() {
        let wide = Ipv4Cidr::parse("172.16.0.0/12").unwrap();
        let inner = Ipv4Cidr::parse("172.30.0.0/16").unwrap();
        let apart = Ipv4Cidr::parse("172.32.0.0/16").unwrap();
        assert!(wide.overlaps(&inner) && inner.overlaps(&wide));
        assert!(wide.contains_net(&inner) && !inner.contains_net(&wide));
        assert!(!wide.overlaps(&apart));
    }

    #[test]
    fn blocked_ranges_cover_management_metadata_and_private_space() {
        for addr in [
            "169.254.169.254",
            "10.0.2.2",
            "192.168.5.2",
            "172.30.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "0.0.0.0",
            "224.0.0.251",
            "255.255.255.255",
        ] {
            assert!(!is_public_ipv4(addr.parse().unwrap()), "{addr}");
        }
        for addr in ["1.1.1.1", "8.8.8.8", "93.184.216.34"] {
            assert!(is_public_ipv4(addr.parse().unwrap()), "{addr}");
        }
        for b in BLOCKED_IPV4 {
            let _ = b.cidr();
        }
    }

    #[test]
    fn allow_rules_refuse_special_ranges_empty_ports_and_ipv6() {
        rule("1.1.1.1/32", &[443]).validate().unwrap();
        rule("1.1.1.0/24", &[80, 443]).validate().unwrap();
        for bad in [
            rule("169.254.169.254/32", &[80]),
            rule("0.0.0.0/0", &[443]),
            rule("10.0.0.0/8", &[443]),
            rule("192.168.1.0/24", &[443]),
            rule("2606:4700::/32", &[443]),
            rule("1.1.1.1/32", &[]),
            rule("1.1.1.1/32", &[0]),
            rule("1.1.1.1/32", &[1; MAX_EGRESS_ALLOW_PORTS + 1]),
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn protocol_defaults_to_tcp_in_json() {
        let r: EgressAllowRule =
            serde_json::from_str(r#"{"cidr":"1.1.1.1/32","ports":[53]}"#).unwrap();
        assert_eq!(r.protocol, EgressProtocol::Tcp);
        let r: EgressAllowRule =
            serde_json::from_str(r#"{"cidr":"1.1.1.1/32","protocol":"udp","ports":[53]}"#).unwrap();
        assert_eq!(r.protocol.as_str(), "udp");
    }
}
