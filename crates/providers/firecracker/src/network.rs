//! Egress profiles `restricted` / `public-web` on the host (PLT-4622,
//! docs/adr/0005-egress-profiles.md).
//!
//! An environment whose revision asks for network egress gets:
//!
//! - a /30 out of [`NetworkConfig::guest_cidr`] (`.1` on the host tap, `.2`
//!   in the guest), recorded in `<env_dir>/net.json`;
//! - a tap device `tsls<11 hex>` created by the provider, with IPv6 disabled
//!   and ICMP redirects / source routing off;
//! - one nftables chain `g_<tap>` in the provider-owned table
//!   `inet tachyon_egress`, reached through the verdict map `guest_taps`.
//!
//! The table is default-deny for anything arriving on a `tsls*` interface: a
//! tap without a map entry hits the final `drop` of `guest_egress`, the node
//! itself (`input`) is unreachable from any tap, nothing may be opened towards
//! a tap (`forward` / `output`, except replies), and IPv6 is dropped. The
//! per-environment chain then drops spoofed sources and every special-purpose
//! IPv4 range ([`tachyon_serverless_domain::BLOCKED_IPV4`]: management
//! network, node, metadata, link-local, RFC1918, CGNAT, loopback, ...) before
//! the profile rules:
//!
//! - `public-web`: DNS (53/tcp+udp) only to [`NetworkConfig::dns_resolver`],
//!   everything else that is left (public IPv4 unicast) accepted;
//! - `restricted`: only the revision's allowlist accepted, the rest dropped.
//!
//! Every change is read back (`nft -j list ...`) and compared with what was
//! meant; [`HostNetwork::setup`] returns a [`VerifiedPolicy`] only when that
//! comparison passed, and the provider re-verifies right before
//! `InstanceStart`.

use std::collections::{BTreeSet, HashSet};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tachyon_serverless_domain::{
    BLOCKED_IPV4, BLOCKED_IPV6, EgressAllowRule, EgressProfile, Ipv4Cidr, is_public_ipv4,
};

use crate::egress_gate::{ExpectedNic, GUEST_IFACE_ID};
use crate::preflight::resolve_command;

/// nftables table owned by the provider (family `inet`).
pub const NFT_TABLE: &str = "tachyon_egress";
/// Comment stored on the table; a table without it is not ours to reuse.
pub const NFT_TABLE_COMMENT: &str = "tachyon-serverless egress v1";
/// Prefix of every tap device the provider creates.
pub const TAP_PREFIX: &str = "tsls";
/// Prefix of the per-environment chain (`g_<tap>`).
pub const CHAIN_PREFIX: &str = "g_";
/// File in the environment directory that records the network lease.
pub const LEASE_FILE: &str = "net.json";
/// Default [`NetworkConfig::guest_cidr`].
pub const DEFAULT_GUEST_CIDR: &str = "172.30.0.0/16";
/// Default [`NetworkConfig::dns_resolver`].
pub const DEFAULT_DNS_RESOLVER: Ipv4Addr = Ipv4Addr::new(1, 1, 1, 1);
/// Bound on every `nft` / `ip` invocation.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
/// `CAP_NET_ADMIN` bit in `CapEff`.
const CAP_NET_ADMIN: u32 = 12;

/// Host network settings of the Firecracker provider.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Pool the per-environment /30s are carved from. Must lie inside a
    /// blocked (non-public) IPv4 range so guest addresses are never routable
    /// and one guest can never reach another.
    pub guest_cidr: Ipv4Cidr,
    /// The only DNS server a `public-web` guest may query (public IPv4).
    pub dns_resolver: Ipv4Addr,
    /// `nft` binary (bare name looked up in `PATH`).
    pub nft_binary: PathBuf,
    /// `ip` binary (iproute2).
    pub ip_binary: PathBuf,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            guest_cidr: Ipv4Cidr::parse(DEFAULT_GUEST_CIDR).expect("valid default"),
            dns_resolver: DEFAULT_DNS_RESOLVER,
            nft_binary: PathBuf::from("nft"),
            ip_binary: PathBuf::from("ip"),
        }
    }
}

impl NetworkConfig {
    /// Refuse settings that would weaken the policy.
    pub fn validate(&self) -> Result<(), String> {
        let pool = &self.guest_cidr;
        if pool.prefix() > 29 {
            return Err(format!(
                "guest_cidr {pool} is too small (at least one /30 is needed)"
            ));
        }
        if !BLOCKED_IPV4.iter().any(|b| b.cidr().contains_net(pool)) {
            return Err(format!(
                "guest_cidr {pool} must lie inside a private / special-purpose range \
                 (e.g. 172.30.0.0/16) so guest addresses are never publicly routable"
            ));
        }
        if !is_public_ipv4(self.dns_resolver) {
            return Err(format!(
                "dns_resolver {} is not a public unicast address; a resolver on the \
                 management network or the node would be reachable from every guest",
                self.dns_resolver
            ));
        }
        Ok(())
    }

    /// Number of /30 leases the pool holds.
    pub fn capacity(&self) -> u32 {
        (self.guest_cidr.size() / 4).min(u32::MAX as u64) as u32
    }
}

// ---------------------------------------------------------------------------
// naming and addressing
// ---------------------------------------------------------------------------

/// Tap name of an environment: `tsls` + 11 hex chars of `sha256(env_id)`
/// (15 bytes, the Linux interface name limit).
pub fn tap_name(env_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(env_id.as_bytes()));
    format!("{TAP_PREFIX}{}", &digest[..11])
}

/// nftables chain of a tap.
pub fn chain_name(tap: &str) -> String {
    format!("{CHAIN_PREFIX}{tap}")
}

/// The network identity of one environment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub env_id: String,
    pub index: u32,
    pub tap: String,
    pub host_ip: Ipv4Addr,
    pub guest_ip: Ipv4Addr,
    pub netmask: Ipv4Addr,
    pub guest_mac: String,
    pub profile: EgressProfile,
    /// Resolver the guest is told about (`public-web` only).
    pub dns_resolver: Option<Ipv4Addr>,
}

impl Lease {
    pub fn new(env_id: &str, index: u32, cfg: &NetworkConfig, profile: EgressProfile) -> Self {
        let base = u32::from(cfg.guest_cidr.network()) + index * 4;
        let host_ip = Ipv4Addr::from(base + 1);
        let guest_ip = Ipv4Addr::from(base + 2);
        let [a, b, c, d] = guest_ip.octets();
        Self {
            env_id: env_id.to_owned(),
            index,
            tap: tap_name(env_id),
            host_ip,
            guest_ip,
            netmask: Ipv4Addr::new(255, 255, 255, 252),
            guest_mac: format!("06:00:{a:02x}:{b:02x}:{c:02x}:{d:02x}"),
            profile,
            dns_resolver: (profile == EgressProfile::PublicWeb).then_some(cfg.dns_resolver),
        }
    }

    pub fn chain(&self) -> String {
        chain_name(&self.tap)
    }

    /// Kernel arguments of a networked guest: `ip=` with the static address,
    /// the gateway on the host tap, no autoconfiguration and the resolver (if
    /// any) as `dns0` (the guest rootfs links `/etc/resolv.conf` to
    /// `/proc/net/pnp`, where the kernel publishes it), plus `ipv6.disable=1`
    /// so the guest has no IPv6 stack at all (the host drops IPv6 as well).
    pub fn kernel_net_args(&self) -> String {
        let mut s = format!(
            "ip={}::{}:{}::{GUEST_IFACE_ID}:off",
            self.guest_ip, self.host_ip, self.netmask
        );
        if let Some(dns) = self.dns_resolver {
            s.push_str(&format!(":{dns}"));
        }
        s.push_str(" ipv6.disable=1");
        s
    }

    pub fn expected_nic(&self) -> ExpectedNic {
        ExpectedNic {
            iface_id: GUEST_IFACE_ID.to_owned(),
            host_dev_name: self.tap.clone(),
        }
    }

    /// Body of `PUT /network-interfaces/eth0`.
    pub fn firecracker_body(&self) -> Value {
        serde_json::json!({
            "iface_id": GUEST_IFACE_ID,
            "host_dev_name": self.tap,
            "guest_mac": self.guest_mac,
        })
    }
}

/// Lowest lease index not in `used`.
pub fn pick_free_index(capacity: u32, used: &HashSet<u32>) -> Option<u32> {
    (0..capacity).find(|i| !used.contains(i))
}

// ---------------------------------------------------------------------------
// nftables scripts
// ---------------------------------------------------------------------------

fn elements(ranges: &[tachyon_serverless_domain::BlockedRange]) -> String {
    ranges.iter().map(|r| r.cidr).collect::<Vec<_>>().join(", ")
}

/// The provider-owned table. Applied only when the table does not exist.
pub fn base_table_script(cfg: &NetworkConfig) -> String {
    format!(
        r#"table inet {NFT_TABLE} {{
  comment "{NFT_TABLE_COMMENT}"
  set blocked_ipv4 {{ type ipv4_addr; flags interval; elements = {{ {v4} }} }}
  set blocked_ipv6 {{ type ipv6_addr; flags interval; elements = {{ {v6} }} }}
  map guest_taps {{ type ifname : verdict; }}
  chain forward {{
    type filter hook forward priority filter - 10; policy accept;
    iifname "{TAP_PREFIX}*" jump guest_egress
    oifname "{TAP_PREFIX}*" ct state established,related counter accept
    oifname "{TAP_PREFIX}*" counter drop
  }}
  chain guest_egress {{
    ip6 daddr @blocked_ipv6 counter drop
    meta nfproto ipv6 counter drop
    iifname vmap @guest_taps
    counter drop
  }}
  chain input {{
    type filter hook input priority filter - 10; policy accept;
    iifname "{TAP_PREFIX}*" counter drop
  }}
  chain output {{
    type filter hook output priority filter - 10; policy accept;
    oifname "{TAP_PREFIX}*" counter drop
  }}
  chain postrouting {{
    type nat hook postrouting priority srcnat; policy accept;
    ip saddr {pool} oifname != "{TAP_PREFIX}*" masquerade
  }}
}}
"#,
        v4 = elements(BLOCKED_IPV4),
        v6 = elements(BLOCKED_IPV6),
        pool = cfg.guest_cidr,
    )
}

/// Final verdict of each rule of a per-environment chain, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Accept,
    Drop,
}

impl Verdict {
    fn key(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Drop => "drop",
        }
    }
}

/// One rule of a per-environment chain: its nft text and what the read-back
/// must show (the verdict and literal values the rule has to contain).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRule {
    pub text: String,
    pub verdict: Verdict,
    pub must_contain: Vec<String>,
}

/// Rules of `g_<tap>` for one lease, in order.
pub fn env_chain_rules(
    lease: &Lease,
    allow: &[EgressAllowRule],
) -> Result<Vec<PlannedRule>, String> {
    let rule = |text: String, verdict: Verdict, must: &[String]| PlannedRule {
        text,
        verdict,
        must_contain: must.to_vec(),
    };
    let mut rules = vec![
        rule(
            "meta nfproto != ipv4 counter drop".into(),
            Verdict::Drop,
            &["nfproto".into()],
        ),
        rule(
            format!("ip saddr != {} counter drop", lease.guest_ip),
            Verdict::Drop,
            &[lease.guest_ip.to_string()],
        ),
        rule(
            "ip daddr @blocked_ipv4 counter drop".into(),
            Verdict::Drop,
            &["@blocked_ipv4".into()],
        ),
    ];
    match lease.profile {
        EgressProfile::None => {
            return Err("egress none has no network device and no chain".into());
        }
        EgressProfile::PublicWeb => {
            if !allow.is_empty() {
                return Err("public-web carries no allowlist".into());
            }
            let dns = lease
                .dns_resolver
                .ok_or("public-web lease without a resolver")?;
            rules.push(rule(
                format!("ip daddr {dns} meta l4proto {{ tcp, udp }} th dport 53 counter accept"),
                Verdict::Accept,
                &[dns.to_string()],
            ));
            rules.push(rule(
                "meta l4proto { tcp, udp } th dport 53 counter drop".into(),
                Verdict::Drop,
                &["dport".into()],
            ));
            rules.push(rule("counter accept".into(), Verdict::Accept, &[]));
        }
        EgressProfile::Restricted => {
            if allow.is_empty() {
                return Err("restricted needs at least one allow rule".into());
            }
            for a in allow {
                a.validate().map_err(|e| e.to_string())?;
                let net = a.network().map_err(|e| e.to_string())?;
                let dst = if net.prefix() == 32 {
                    net.network().to_string()
                } else {
                    net.to_string()
                };
                let ports: BTreeSet<u16> = a.ports.iter().copied().collect();
                let ports_text = ports
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                rules.push(rule(
                    format!(
                        "ip daddr {dst} {proto} dport {{ {ports_text} }} counter accept",
                        proto = a.protocol.as_str()
                    ),
                    Verdict::Accept,
                    &[net.network().to_string(), a.protocol.as_str().to_owned()],
                ));
            }
            rules.push(rule("counter drop".into(), Verdict::Drop, &[]));
        }
    }
    Ok(rules)
}

/// Transaction that (re)creates `g_<tap>` and maps the tap to it.
pub fn env_chain_script(lease: &Lease, rules: &[PlannedRule]) -> String {
    let chain = lease.chain();
    let mut s =
        format!("add chain inet {NFT_TABLE} {chain}\nflush chain inet {NFT_TABLE} {chain}\n");
    for r in rules {
        s.push_str(&format!("add rule inet {NFT_TABLE} {chain} {}\n", r.text));
    }
    s.push_str(&format!(
        "add element inet {NFT_TABLE} guest_taps {{ \"{}\" : jump {chain} }}\n",
        lease.tap
    ));
    s
}

// ---------------------------------------------------------------------------
// read-back
// ---------------------------------------------------------------------------

fn nft_objects<'a>(json: &'a Value, kind: &'a str) -> impl Iterator<Item = &'a Value> + 'a {
    json.get("nftables")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(move |o| o.get(kind))
}

fn rules_of<'a>(json: &'a Value, chain: &'a str) -> Vec<&'a Value> {
    nft_objects(json, "rule")
        .filter(|r| r.get("chain").and_then(Value::as_str) == Some(chain))
        .collect()
}

fn rule_verdict(rule: &Value) -> Option<Verdict> {
    let last = rule.get("expr")?.as_array()?.last()?;
    if last.get("accept").is_some() {
        Some(Verdict::Accept)
    } else if last.get("drop").is_some() {
        Some(Verdict::Drop)
    } else {
        None
    }
}

/// Compare `nft -j list table inet tachyon_egress` with the base structure.
pub fn verify_base_table(json: &Value) -> Result<(), String> {
    let table = nft_objects(json, "table")
        .find(|t| t.get("name").and_then(Value::as_str) == Some(NFT_TABLE))
        .ok_or("the egress table is missing")?;
    if table.get("comment").and_then(Value::as_str) != Some(NFT_TABLE_COMMENT) {
        return Err(format!(
            "table inet {NFT_TABLE} exists without the comment `{NFT_TABLE_COMMENT}` (not ours or another version)"
        ));
    }
    let chain = |name: &str| {
        nft_objects(json, "chain").find(|c| c.get("name").and_then(Value::as_str) == Some(name))
    };
    for (name, kind, hook) in [
        ("forward", "filter", "forward"),
        ("input", "filter", "input"),
        ("output", "filter", "output"),
        ("postrouting", "nat", "postrouting"),
    ] {
        let c = chain(name).ok_or_else(|| format!("base chain `{name}` is missing"))?;
        if c.get("type").and_then(Value::as_str) != Some(kind)
            || c.get("hook").and_then(Value::as_str) != Some(hook)
        {
            return Err(format!(
                "base chain `{name}` is not a {kind} chain on hook {hook}"
            ));
        }
    }
    chain("guest_egress").ok_or("chain `guest_egress` is missing")?;
    let expect_rules = |name: &str, verdicts: &[Option<Verdict>]| -> Result<(), String> {
        let rules = rules_of(json, name);
        let got: Vec<Option<Verdict>> = rules.iter().map(|r| rule_verdict(r)).collect();
        if got != verdicts {
            return Err(format!(
                "chain `{name}` has verdicts {got:?}, expected {verdicts:?}"
            ));
        }
        Ok(())
    };
    // forward: jump guest_egress, accept replies, drop the rest.
    expect_rules(
        "forward",
        &[None, Some(Verdict::Accept), Some(Verdict::Drop)],
    )?;
    let first = rules_of(json, "forward")[0].to_string();
    if !first.contains("\"guest_egress\"") {
        return Err("the first forward rule does not jump to guest_egress".into());
    }
    // guest_egress: v6 drops, the tap map, final drop.
    expect_rules(
        "guest_egress",
        &[
            Some(Verdict::Drop),
            Some(Verdict::Drop),
            None,
            Some(Verdict::Drop),
        ],
    )?;
    if !rules_of(json, "guest_egress")[2]
        .to_string()
        .contains("@guest_taps")
    {
        return Err("guest_egress does not dispatch through @guest_taps".into());
    }
    expect_rules("input", &[Some(Verdict::Drop)])?;
    expect_rules("output", &[Some(Verdict::Drop)])?;
    if rules_of(json, "postrouting").len() != 1 {
        return Err("postrouting must hold exactly the masquerade rule".into());
    }
    let set_len = |name: &str| {
        nft_objects(json, "set")
            .find(|s| s.get("name").and_then(Value::as_str) == Some(name))
            .and_then(|s| s.get("elem"))
            .and_then(Value::as_array)
            .map(Vec::len)
    };
    if set_len("blocked_ipv4") != Some(BLOCKED_IPV4.len()) {
        return Err(format!(
            "set blocked_ipv4 has {:?} elements, expected {}",
            set_len("blocked_ipv4"),
            BLOCKED_IPV4.len()
        ));
    }
    if set_len("blocked_ipv6") != Some(BLOCKED_IPV6.len()) {
        return Err(format!(
            "set blocked_ipv6 has {:?} elements, expected {}",
            set_len("blocked_ipv6"),
            BLOCKED_IPV6.len()
        ));
    }
    Ok(())
}

/// Taps present in the `guest_taps` map, with the chain each jumps to.
pub fn mapped_taps(json: &Value) -> Vec<(String, String)> {
    nft_objects(json, "map")
        .filter(|m| m.get("name").and_then(Value::as_str) == Some("guest_taps"))
        .filter_map(|m| m.get("elem").and_then(Value::as_array))
        .flatten()
        .filter_map(|e| {
            let pair = e.as_array()?;
            let tap = pair.first()?.as_str()?.to_owned();
            let target = pair
                .get(1)?
                .get("jump")?
                .get("target")?
                .as_str()?
                .to_owned();
            Some((tap, target))
        })
        .collect()
}

/// Names of the per-environment chains present in the table.
pub fn env_chains(json: &Value) -> Vec<String> {
    nft_objects(json, "chain")
        .filter_map(|c| c.get("name").and_then(Value::as_str))
        .filter(|n| n.starts_with(CHAIN_PREFIX))
        .map(str::to_owned)
        .collect()
}

/// Compare the table read back after installing a lease with the plan.
pub fn verify_env_policy(json: &Value, lease: &Lease, plan: &[PlannedRule]) -> Result<(), String> {
    verify_base_table(json)?;
    let chain = lease.chain();
    let rules = rules_of(json, &chain);
    if rules.len() != plan.len() {
        return Err(format!(
            "chain {chain} has {} rules, expected {}",
            rules.len(),
            plan.len()
        ));
    }
    for (i, (got, want)) in rules.iter().zip(plan).enumerate() {
        if rule_verdict(got) != Some(want.verdict) {
            return Err(format!(
                "rule {i} of {chain} ends in {:?}, expected {}",
                rule_verdict(got),
                want.verdict.key()
            ));
        }
        let text = got.to_string();
        if let Some(missing) = want
            .must_contain
            .iter()
            .find(|m| !text.contains(m.as_str()))
        {
            return Err(format!(
                "rule {i} of {chain} does not contain `{missing}`: {text}"
            ));
        }
    }
    let mapped = mapped_taps(json);
    match mapped
        .iter()
        .filter(|(tap, _)| tap == &lease.tap)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [(_, target)] if *target == chain => Ok(()),
        [] => Err(format!("tap {} is not in the guest_taps map", lease.tap)),
        other => Err(format!(
            "tap {} maps to {:?}, expected exactly {chain}",
            lease.tap, other
        )),
    }
}

// ---------------------------------------------------------------------------
// host capability
// ---------------------------------------------------------------------------

/// Parse `CapEff:` from `/proc/self/status`.
pub fn parse_cap_eff(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|l| l.strip_prefix("CapEff:"))
        .and_then(|v| u64::from_str_radix(v.trim(), 16).ok())
}

/// Whether this process may create taps and edit nftables here: Linux,
/// `CAP_NET_ADMIN`, and the `nft` / `ip` binaries. Returns their paths.
pub fn host_can_manage(cfg: &NetworkConfig) -> Result<(PathBuf, PathBuf), String> {
    if !cfg!(target_os = "linux") {
        return Err(format!(
            "host os is {}; tap devices and nftables need linux",
            std::env::consts::OS
        ));
    }
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|e| format!("cannot read /proc/self/status: {e}"))?;
    let cap = parse_cap_eff(&status).ok_or("no CapEff in /proc/self/status")?;
    if cap & (1 << CAP_NET_ADMIN) == 0 {
        return Err("the gateway lacks CAP_NET_ADMIN (run as root or grant the capability)".into());
    }
    let nft = resolve_command(&cfg.nft_binary)
        .ok_or_else(|| format!("{} not found (install nftables)", cfg.nft_binary.display()))?;
    let ip = resolve_command(&cfg.ip_binary)
        .ok_or_else(|| format!("{} not found (install iproute2)", cfg.ip_binary.display()))?;
    Ok((nft, ip))
}

/// Whether this process can enforce the network profiles on this host. The
/// `Err` is the reason reported as the capability's `Unsupported` note and in
/// preflight.
pub fn host_support(cfg: &NetworkConfig) -> Result<String, String> {
    cfg.validate()?;
    let (nft, ip) = host_can_manage(cfg)?;
    if !Path::new("/dev/net/tun").exists() {
        return Err("/dev/net/tun does not exist (tun module not loaded)".into());
    }
    let forward = std::fs::read_to_string("/proc/sys/net/ipv4/ip_forward").unwrap_or_default();
    if forward.trim() != "1" {
        return Err("net.ipv4.ip_forward is not 1 (guests could not reach anything)".into());
    }
    Ok(format!(
        "CAP_NET_ADMIN, {} and {}, /dev/net/tun, ip_forward=1; pool {}, resolver {}",
        nft.display(),
        ip.display(),
        cfg.guest_cidr,
        cfg.dns_resolver
    ))
}

// ---------------------------------------------------------------------------
// host operations
// ---------------------------------------------------------------------------

/// A policy that was installed and read back. Only [`HostNetwork::setup`]
/// constructs one.
#[derive(Debug, Clone)]
pub struct VerifiedPolicy {
    lease: Lease,
    plan: Vec<PlannedRule>,
    verified_ms: u64,
}

impl VerifiedPolicy {
    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    pub fn rule_count(&self) -> usize {
        self.plan.len()
    }

    /// Time from the start of setup to the successful read-back.
    pub fn verified_ms(&self) -> u64 {
        self.verified_ms
    }
}

/// What [`HostNetwork::teardown`] removed.
#[derive(Debug, Default, Clone)]
pub struct Teardown {
    pub cleaned: Vec<String>,
    /// `nft list chain` of the environment chain (with counters) before it
    /// was deleted, when it existed.
    pub counters: Option<String>,
}

/// Runs `nft` / `ip` for the provider. All mutations are serialised.
#[derive(Debug)]
pub struct HostNetwork {
    cfg: NetworkConfig,
    lock: tokio::sync::Mutex<()>,
}

impl HostNetwork {
    pub fn new(cfg: NetworkConfig) -> Self {
        Self {
            cfg,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn config(&self) -> &NetworkConfig {
        &self.cfg
    }

    async fn run(&self, bin: &Path, args: &[&str], stdin: Option<&str>) -> Result<String, String> {
        use tokio::io::AsyncWriteExt;
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(args)
            .stdin(if stdin.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let what = format!("{} {}", bin.display(), args.join(" "));
        let mut child = cmd.spawn().map_err(|e| format!("{what}: {e}"))?;
        if let Some(input) = stdin {
            let mut pipe = child.stdin.take().ok_or("no stdin pipe")?;
            pipe.write_all(input.as_bytes())
                .await
                .map_err(|e| format!("{what}: write stdin: {e}"))?;
            drop(pipe);
        }
        let out = tokio::time::timeout(COMMAND_TIMEOUT, child.wait_with_output())
            .await
            .map_err(|_| format!("{what}: timed out after {COMMAND_TIMEOUT:?}"))?
            .map_err(|e| format!("{what}: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "{what} failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    async fn nft_apply(&self, script: &str) -> Result<(), String> {
        self.run(&self.cfg.nft_binary, &["-f", "-"], Some(script))
            .await
            .map(|_| ())
            .map_err(|e| format!("{e}\n--- script ---\n{script}"))
    }

    /// `nft -j list table inet tachyon_egress`, or `None` when it does not exist.
    async fn read_table(&self) -> Result<Option<Value>, String> {
        match self
            .run(
                &self.cfg.nft_binary,
                &["-j", "list", "table", "inet", NFT_TABLE],
                None,
            )
            .await
        {
            Ok(out) => serde_json::from_str(&out)
                .map(Some)
                .map_err(|e| format!("nft -j output is not JSON: {e}")),
            Err(e) if e.contains("No such file or directory") => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn ip(&self, args: &[&str]) -> Result<(), String> {
        self.run(&self.cfg.ip_binary, args, None).await.map(|_| ())
    }

    fn tap_exists(tap: &str) -> bool {
        Path::new("/sys/class/net").join(tap).exists()
    }

    fn set_sysctl(path: &str, value: &str) -> Result<(), String> {
        std::fs::write(path, value).map_err(|e| format!("write {value} to {path}: {e}"))
    }

    /// Leases recorded in `<workdir>/*/net.json`.
    fn leases_in(workdir: &Path) -> Vec<Lease> {
        let Ok(rd) = std::fs::read_dir(workdir) else {
            return Vec::new();
        };
        rd.filter_map(Result::ok)
            .filter_map(|e| std::fs::read(e.path().join(LEASE_FILE)).ok())
            .filter_map(|b| serde_json::from_slice(&b).ok())
            .collect()
    }

    async fn ensure_base_table(&self) -> Result<Value, String> {
        if self.read_table().await?.is_none() {
            self.nft_apply(&base_table_script(&self.cfg)).await?;
        }
        let table = self
            .read_table()
            .await?
            .ok_or("the egress table is still missing after it was created")?;
        verify_base_table(&table)?;
        Ok(table)
    }

    /// Allocate a lease, create the tap, install the chain and read it back.
    /// On `Err` the caller must call [`Self::teardown`] for the environment.
    pub async fn setup(
        &self,
        workdir: &Path,
        env_dir: &Path,
        env_id: &str,
        profile: EgressProfile,
        allow: &[EgressAllowRule],
    ) -> Result<VerifiedPolicy, String> {
        let started = std::time::Instant::now();
        host_support(&self.cfg)?;
        let _guard = self.lock.lock().await;
        self.ensure_base_table().await?;

        let used: HashSet<u32> = Self::leases_in(workdir).iter().map(|l| l.index).collect();
        let index = pick_free_index(self.cfg.capacity(), &used)
            .ok_or_else(|| format!("guest_cidr {} has no free /30", self.cfg.guest_cidr))?;
        let lease = Lease::new(env_id, index, &self.cfg, profile);
        let plan = env_chain_rules(&lease, allow)?;
        let json = serde_json::to_vec_pretty(&lease).map_err(|e| e.to_string())?;
        std::fs::write(env_dir.join(LEASE_FILE), json)
            .map_err(|e| format!("write {}: {e}", env_dir.join(LEASE_FILE).display()))?;

        if Self::tap_exists(&lease.tap) {
            return Err(format!("tap {} already exists", lease.tap));
        }
        self.ip(&["tuntap", "add", "dev", &lease.tap, "mode", "tap"])
            .await?;
        let tap = &lease.tap;
        Self::set_sysctl(&format!("/proc/sys/net/ipv6/conf/{tap}/disable_ipv6"), "1")?;
        for (key, value) in [
            ("accept_redirects", "0"),
            ("send_redirects", "0"),
            ("accept_source_route", "0"),
            ("proxy_arp", "0"),
            ("rp_filter", "1"),
        ] {
            Self::set_sysctl(&format!("/proc/sys/net/ipv4/conf/{tap}/{key}"), value)?;
        }
        self.ip(&["addr", "add", &format!("{}/30", lease.host_ip), "dev", tap])
            .await?;

        self.nft_apply(&env_chain_script(&lease, &plan)).await?;
        let table = self
            .read_table()
            .await?
            .ok_or("the egress table disappeared while installing the policy")?;
        verify_env_policy(&table, &lease, &plan)?;
        // The link comes up only once its policy is confirmed.
        self.ip(&["link", "set", "dev", tap, "up"]).await?;
        Ok(VerifiedPolicy {
            lease,
            plan,
            verified_ms: started.elapsed().as_millis() as u64,
        })
    }

    /// Read the policy back again (right before `InstanceStart`).
    pub async fn verify(&self, policy: &VerifiedPolicy) -> Result<(), String> {
        let table = self
            .read_table()
            .await?
            .ok_or("the egress table is missing")?;
        verify_env_policy(&table, &policy.lease, &policy.plan)?;
        if !Self::tap_exists(&policy.lease.tap) {
            return Err(format!("tap {} is missing", policy.lease.tap));
        }
        Ok(())
    }

    /// Remove the map entry, chain and tap of one environment (whatever of
    /// it exists) and prove they are gone. When no other environment under
    /// `workdir` holds a lease, the table is removed too.
    pub async fn teardown(&self, workdir: &Path, env_id: &str) -> Result<Teardown, String> {
        let mut report = Teardown::default();
        if !cfg!(target_os = "linux") {
            return Ok(report);
        }
        let tap = tap_name(env_id);
        let chain = chain_name(&tap);
        let _guard = self.lock.lock().await;
        let table = match self.read_table().await {
            Ok(t) => t,
            // No permission / no nft: nothing this process could have created.
            Err(_) if !Self::tap_exists(&tap) => return Ok(report),
            Err(e) => return Err(e),
        };
        if let Some(table) = &table {
            let mut script = String::new();
            if mapped_taps(table).iter().any(|(t, _)| *t == tap) {
                script.push_str(&format!(
                    "delete element inet {NFT_TABLE} guest_taps {{ \"{tap}\" }}\n"
                ));
                report.cleaned.push(format!("nft-map:{tap}"));
            }
            if env_chains(table).contains(&chain) {
                report.counters = self
                    .run(
                        &self.cfg.nft_binary,
                        &["list", "chain", "inet", NFT_TABLE, &chain],
                        None,
                    )
                    .await
                    .ok();
                script.push_str(&format!("delete chain inet {NFT_TABLE} {chain}\n"));
                report.cleaned.push(format!("nft-chain:{chain}"));
            }
            if !script.is_empty() {
                self.nft_apply(&script).await?;
            }
        }
        if Self::tap_exists(&tap) {
            self.ip(&["link", "del", "dev", &tap]).await?;
            report.cleaned.push(format!("tap:{tap}"));
        }
        // Prove it.
        if Self::tap_exists(&tap) {
            return Err(format!("tap {tap} still exists after deletion"));
        }
        if let Some(table) = self.read_table().await? {
            if env_chains(&table).contains(&chain)
                || mapped_taps(&table).iter().any(|(t, _)| *t == tap)
            {
                return Err(format!("nft chain or map entry for {tap} still exists"));
            }
            let others = Self::leases_in(workdir)
                .into_iter()
                .any(|l| l.env_id != env_id);
            if !others && mapped_taps(&table).is_empty() && env_chains(&table).is_empty() {
                self.nft_apply(&format!("delete table inet {NFT_TABLE}\n"))
                    .await?;
                report.cleaned.push(format!("nft-table:{NFT_TABLE}"));
            }
        }
        Ok(report)
    }

    /// Remove taps, chains and map entries that belong to no environment
    /// directory under `workdir` (a crash between setup and cleanup), and the
    /// table itself when nothing uses it. `live_env_ids` are the environment
    /// directories that exist. Returns what was removed.
    pub async fn sweep(
        &self,
        workdir: &Path,
        live_env_ids: &[String],
    ) -> Result<Vec<String>, String> {
        if host_can_manage(&self.cfg).is_err() {
            return Ok(Vec::new());
        }
        let live: HashSet<String> = live_env_ids.iter().map(|id| tap_name(id)).collect();
        let mut orphans: BTreeSet<String> = BTreeSet::new();
        if let Ok(rd) = std::fs::read_dir("/sys/class/net") {
            for e in rd.filter_map(Result::ok) {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with(TAP_PREFIX) && !live.contains(&name) {
                    orphans.insert(name);
                }
            }
        }
        let mut removed = Vec::new();
        {
            let _guard = self.lock.lock().await;
            let Some(table) = self.read_table().await? else {
                drop(_guard);
                for tap in &orphans {
                    self.ip(&["link", "del", "dev", tap]).await?;
                    removed.push(format!("tap:{tap}"));
                }
                return Ok(removed);
            };
            let mut script = String::new();
            for (tap, _) in mapped_taps(&table) {
                if !live.contains(&tap) {
                    script.push_str(&format!(
                        "delete element inet {NFT_TABLE} guest_taps {{ \"{tap}\" }}\n"
                    ));
                    removed.push(format!("nft-map:{tap}"));
                }
            }
            for chain in env_chains(&table) {
                let tap = chain.trim_start_matches(CHAIN_PREFIX);
                if !live.contains(tap) {
                    script.push_str(&format!("delete chain inet {NFT_TABLE} {chain}\n"));
                    removed.push(format!("nft-chain:{chain}"));
                }
            }
            if !script.is_empty() {
                self.nft_apply(&script).await?;
            }
            for tap in &orphans {
                self.ip(&["link", "del", "dev", tap]).await?;
                removed.push(format!("tap:{tap}"));
            }
            let in_use = Self::leases_in(workdir)
                .iter()
                .any(|l| live.contains(&l.tap));
            if !in_use {
                // Also replaces a table left by an older provider version.
                self.nft_apply(&format!("delete table inet {NFT_TABLE}\n"))
                    .await?;
                removed.push(format!("nft-table:{NFT_TABLE}"));
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tachyon_serverless_domain::EgressProtocol;

    fn cfg() -> NetworkConfig {
        NetworkConfig::default()
    }

    fn allow(cidr: &str, proto: EgressProtocol, ports: &[u16]) -> EgressAllowRule {
        EgressAllowRule {
            cidr: cidr.into(),
            protocol: proto,
            ports: ports.to_vec(),
        }
    }

    #[test]
    fn config_defaults_are_valid_and_unsafe_settings_are_refused() {
        cfg().validate().unwrap();
        assert_eq!(cfg().capacity(), 16384);
        let mut c = cfg();
        c.guest_cidr = Ipv4Cidr::parse("8.8.0.0/16").unwrap();
        assert!(c.validate().is_err(), "public pool");
        let mut c = cfg();
        c.guest_cidr = Ipv4Cidr::parse("172.30.0.0/30").unwrap();
        assert!(c.validate().is_err(), "pool too small");
        for resolver in ["10.0.0.53", "127.0.0.53", "169.254.169.254", "192.168.5.3"] {
            let mut c = cfg();
            c.dns_resolver = resolver.parse().unwrap();
            assert!(c.validate().is_err(), "{resolver}");
        }
    }

    #[test]
    fn tap_names_fit_ifnamsiz_and_are_stable() {
        let t = tap_name("env_01hzzzzzzzzzzzzzzzzzzzzzzz");
        assert_eq!(t.len(), 15);
        assert!(t.starts_with(TAP_PREFIX));
        assert_eq!(t, tap_name("env_01hzzzzzzzzzzzzzzzzzzzzzzz"));
        assert_ne!(t, tap_name("env_01hzzzzzzzzzzzzzzzzzzzzzzy"));
        assert_eq!(chain_name(&t), format!("g_{t}"));
    }

    #[test]
    fn leases_are_disjoint_slash30s() {
        let a = Lease::new("env_a", 0, &cfg(), EgressProfile::PublicWeb);
        let b = Lease::new("env_b", 1, &cfg(), EgressProfile::Restricted);
        assert_eq!(a.host_ip, Ipv4Addr::new(172, 30, 0, 1));
        assert_eq!(a.guest_ip, Ipv4Addr::new(172, 30, 0, 2));
        assert_eq!(b.host_ip, Ipv4Addr::new(172, 30, 0, 5));
        assert_eq!(b.guest_ip, Ipv4Addr::new(172, 30, 0, 6));
        assert_eq!(a.guest_mac, "06:00:ac:1e:00:02");
        assert_eq!(
            a.kernel_net_args(),
            "ip=172.30.0.2::172.30.0.1:255.255.255.252::eth0:off:1.1.1.1 ipv6.disable=1"
        );
        assert_eq!(
            b.kernel_net_args(),
            "ip=172.30.0.6::172.30.0.5:255.255.255.252::eth0:off ipv6.disable=1",
            "restricted guests are told no resolver"
        );
        assert_eq!(
            a.firecracker_body(),
            json!({"iface_id": "eth0", "host_dev_name": a.tap, "guest_mac": "06:00:ac:1e:00:02"})
        );
        let used: HashSet<u32> = [0, 1, 3].into_iter().collect();
        assert_eq!(pick_free_index(4, &used), Some(2));
        assert_eq!(pick_free_index(2, &used), None);
    }

    #[test]
    fn public_web_denies_special_ranges_and_foreign_dns_before_accepting() {
        let lease = Lease::new("env_a", 0, &cfg(), EgressProfile::PublicWeb);
        let rules = env_chain_rules(&lease, &[]).unwrap();
        let verdicts: Vec<Verdict> = rules.iter().map(|r| r.verdict).collect();
        use Verdict::*;
        assert_eq!(verdicts, vec![Drop, Drop, Drop, Accept, Drop, Accept]);
        assert!(rules[1].text.contains("ip saddr != 172.30.0.2"));
        assert!(rules[2].text.contains("@blocked_ipv4"));
        assert!(rules[3].text.contains("ip daddr 1.1.1.1") && rules[3].text.contains("dport 53"));
        assert!(env_chain_rules(&lease, &[allow("1.1.1.1", EgressProtocol::Tcp, &[443])]).is_err());
        let script = env_chain_script(&lease, &rules);
        assert!(script.starts_with(&format!("add chain inet tachyon_egress g_{}\n", lease.tap)));
        assert!(script.ends_with(&format!(
            "add element inet tachyon_egress guest_taps {{ \"{0}\" : jump g_{0} }}\n",
            lease.tap
        )));
    }

    #[test]
    fn restricted_accepts_only_the_allowlist_and_ends_in_drop() {
        let lease = Lease::new("env_r", 2, &cfg(), EgressProfile::Restricted);
        let rules = env_chain_rules(
            &lease,
            &[
                allow("1.1.1.1/32", EgressProtocol::Tcp, &[443, 80, 443]),
                allow("93.184.216.0/24", EgressProtocol::Udp, &[53]),
            ],
        )
        .unwrap();
        let texts: Vec<&str> = rules.iter().map(|r| r.text.as_str()).collect();
        assert_eq!(
            texts[3],
            "ip daddr 1.1.1.1 tcp dport { 80, 443 } counter accept"
        );
        assert_eq!(
            texts[4],
            "ip daddr 93.184.216.0/24 udp dport { 53 } counter accept"
        );
        assert_eq!(texts[5], "counter drop");
        assert!(env_chain_rules(&lease, &[]).is_err());
        assert!(
            env_chain_rules(
                &lease,
                &[allow("169.254.169.254", EgressProtocol::Tcp, &[80])]
            )
            .is_err(),
            "the provider re-validates the allowlist"
        );
        let none = Lease::new("env_n", 0, &cfg(), EgressProfile::None);
        assert!(env_chain_rules(&none, &[]).is_err());
    }

    #[test]
    fn base_script_is_default_deny_for_taps() {
        let s = base_table_script(&cfg());
        assert!(s.contains("comment \"tachyon-serverless egress v1\""));
        assert!(s.contains("169.254.0.0/16") && s.contains("fe80::/10"));
        assert!(
            s.contains("iifname \"tsls*\" counter drop"),
            "node unreachable"
        );
        assert!(s.contains("oifname \"tsls*\" counter drop"), "no inbound");
        assert!(s.contains("ip saddr 172.30.0.0/16 oifname != \"tsls*\" masquerade"));
        assert!(s.contains("meta nfproto ipv6 counter drop"));
    }

    fn rule(chain: &str, expr: Value) -> Value {
        json!({"rule": {"family": "inet", "table": NFT_TABLE, "chain": chain, "expr": expr}})
    }

    /// Shaped after `nft -j list table` of nftables 1.1.6.
    fn table_json(lease: &Lease, plan: &[PlannedRule]) -> Value {
        let prefix = |c: &str| {
            let (a, l) = c.split_once('/').unwrap();
            json!({"prefix": {"addr": a, "len": l.parse::<u8>().unwrap()}})
        };
        let chain = |name: &str, extra: Value| {
            let mut c = json!({"family": "inet", "table": NFT_TABLE, "name": name});
            if let Value::Object(m) = extra {
                c.as_object_mut().unwrap().extend(m);
            }
            json!({"chain": c})
        };
        let counter = json!({"counter": {"packets": 0, "bytes": 0}});
        let mut objs = vec![
            json!({"metainfo": {"version": "1.1.6"}}),
            json!({"table": {"family": "inet", "name": NFT_TABLE, "comment": NFT_TABLE_COMMENT}}),
            chain(
                "forward",
                json!({"type": "filter", "hook": "forward", "prio": -10, "policy": "accept"}),
            ),
            chain("guest_egress", json!({})),
            chain(
                "input",
                json!({"type": "filter", "hook": "input", "prio": -10, "policy": "accept"}),
            ),
            chain(
                "output",
                json!({"type": "filter", "hook": "output", "prio": -10, "policy": "accept"}),
            ),
            chain(
                "postrouting",
                json!({"type": "nat", "hook": "postrouting", "prio": 100, "policy": "accept"}),
            ),
            chain(&lease.chain(), json!({})),
            json!({"set": {"name": "blocked_ipv4", "elem": BLOCKED_IPV4.iter().map(|b| prefix(b.cidr)).collect::<Vec<_>>()}}),
            json!({"set": {"name": "blocked_ipv6", "elem": BLOCKED_IPV6.iter().map(|b| prefix(b.cidr)).collect::<Vec<_>>()}}),
            json!({"map": {"name": "guest_taps", "type": "ifname", "map": "verdict",
                "elem": [[lease.tap, {"jump": {"target": lease.chain()}}]]}}),
            rule(
                "forward",
                json!([{"match": {"left": {"meta": {"key": "iifname"}}, "right": "tsls*"}}, {"jump": {"target": "guest_egress"}}]),
            ),
            rule("forward", json!([{"match": {}}, counter, {"accept": null}])),
            rule("forward", json!([{"match": {}}, counter, {"drop": null}])),
            rule(
                "guest_egress",
                json!([{"match": {"right": "@blocked_ipv6"}}, counter, {"drop": null}]),
            ),
            rule(
                "guest_egress",
                json!([{"match": {"right": "ipv6"}}, counter, {"drop": null}]),
            ),
            rule(
                "guest_egress",
                json!([{"vmap": {"key": {"meta": {"key": "iifname"}}, "data": "@guest_taps"}}]),
            ),
            rule("guest_egress", json!([counter, {"drop": null}])),
            rule("input", json!([{"match": {}}, counter, {"drop": null}])),
            rule("output", json!([{"match": {}}, counter, {"drop": null}])),
            rule("postrouting", json!([{"masquerade": null}])),
        ];
        for p in plan {
            let v = if p.verdict == Verdict::Accept {
                json!({"accept": null})
            } else {
                json!({"drop": null})
            };
            let lits: Vec<Value> = p
                .must_contain
                .iter()
                .map(|m| json!({"match": {"right": m}}))
                .collect();
            let mut expr = lits;
            expr.push(counter.clone());
            expr.push(v);
            objs.push(rule(&lease.chain(), Value::Array(expr)));
        }
        json!({"nftables": objs})
    }

    #[test]
    fn read_back_accepts_the_installed_policy() {
        let lease = Lease::new("env_a", 0, &cfg(), EgressProfile::PublicWeb);
        let plan = env_chain_rules(&lease, &[]).unwrap();
        let table = table_json(&lease, &plan);
        verify_base_table(&table).unwrap();
        verify_env_policy(&table, &lease, &plan).unwrap();
        assert_eq!(
            mapped_taps(&table),
            vec![(lease.tap.clone(), lease.chain())]
        );
        assert_eq!(env_chains(&table), vec![lease.chain()]);
    }

    #[test]
    fn read_back_fails_closed_on_any_divergence() {
        let lease = Lease::new("env_a", 0, &cfg(), EgressProfile::PublicWeb);
        let plan = env_chain_rules(&lease, &[]).unwrap();
        let good = table_json(&lease, &plan);

        // A missing final rule (the chain would fall through).
        let mut short = plan.clone();
        short.pop();
        assert!(verify_env_policy(&table_json(&lease, &short), &lease, &plan).is_err());

        // A verdict flipped: the blocked-range drop became accept.
        let mut flipped = plan.clone();
        flipped[2].verdict = Verdict::Accept;
        assert!(verify_env_policy(&table_json(&lease, &flipped), &lease, &plan).is_err());

        // Anti-spoof rule for another guest's address.
        let other = Lease::new("env_b", 1, &cfg(), EgressProfile::PublicWeb);
        let other_plan = env_chain_rules(&other, &[]).unwrap();
        let mut spoof = plan.clone();
        spoof[1] = other_plan[1].clone();
        assert!(verify_env_policy(&table_json(&lease, &spoof), &lease, &plan).is_err());

        // The tap is not mapped to its chain.
        let mut unmapped = good.clone();
        for o in unmapped["nftables"].as_array_mut().unwrap() {
            if o.get("map").is_some() {
                o["map"]["elem"] = json!([]);
            }
        }
        assert!(verify_env_policy(&unmapped, &lease, &plan).is_err());

        // Base structure: wrong comment, missing input drop, empty blocked set.
        let mut foreign = good.clone();
        foreign["nftables"][1]["table"]["comment"] = json!("someone else");
        assert!(verify_base_table(&foreign).is_err());
        let mut no_input = good.clone();
        no_input["nftables"]
            .as_array_mut()
            .unwrap()
            .retain(|o| o.pointer("/rule/chain") != Some(&json!("input")));
        assert!(verify_base_table(&no_input).is_err());
        let mut no_set = good.clone();
        for o in no_set["nftables"].as_array_mut().unwrap() {
            if o.pointer("/set/name") == Some(&json!("blocked_ipv4")) {
                o["set"]["elem"] = json!([]);
            }
        }
        assert!(verify_base_table(&no_set).is_err());
        assert!(verify_base_table(&json!({"nftables": []})).is_err());
    }

    #[test]
    fn cap_eff_parsing() {
        let status = "Name:\tgateway\nCapInh:\t0000000000000000\nCapEff:\t000001ffffffffff\n";
        let cap = parse_cap_eff(status).unwrap();
        assert_ne!(cap & (1 << CAP_NET_ADMIN), 0);
        assert_eq!(parse_cap_eff("CapEff:\t0000000000000000\n"), Some(0));
        assert_eq!(parse_cap_eff("nothing"), None);
    }

    #[test]
    fn host_support_reports_a_reason_when_unavailable() {
        // On macOS or as an unprivileged user this must be an Err with a reason;
        // on a privileged Linux host with ip_forward=1 it may be Ok.
        match host_support(&cfg()) {
            Ok(detail) => assert!(detail.contains("CAP_NET_ADMIN")),
            Err(reason) => assert!(!reason.is_empty()),
        }
        let mut bad = cfg();
        bad.dns_resolver = Ipv4Addr::new(10, 0, 0, 1);
        assert!(host_support(&bad).unwrap_err().contains("dns_resolver"));
    }
}
