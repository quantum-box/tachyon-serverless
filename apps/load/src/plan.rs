//! Load plans: phases given on the command line, and the seeded jitter.

use serde::{Deserialize, Serialize};

use crate::limits::LoadLimits;

/// What a phase does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PhaseKind {
    /// `requests` invocations by `concurrency` workers.
    Invoke {
        tenant: String,
        concurrency: u32,
        requests: u64,
        /// Handler time asked of the function, milliseconds.
        handler_ms: u64,
        /// Uniform jitter added to `handler_ms` and waited before each send
        /// (0..jitter_ms), drawn from the seed.
        jitter_ms: u64,
    },
    /// Send nothing; poll `GET /v1/capacity` until the node reports zero
    /// environments, nothing in flight and nothing queued, or `timeout_ms`.
    IdleUntilZero { tenant: String, timeout_ms: u64 },
    /// Send nothing for `ms`.
    Pause { ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Phase {
    pub name: String,
    /// Consecutive phases with the same wave run at the same time (two
    /// tenants at once); `None` runs alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wave: Option<u32>,
    #[serde(flatten)]
    pub kind: PhaseKind,
}

/// Parse `name=burst,tenant=a,concurrency=8,requests=24,handler_ms=400,jitter_ms=50`,
/// `name=idle,idle_until_zero_ms=30000,tenant=a` or `name=wait,pause_ms=2000`.
pub fn parse_phase(spec: &str) -> Result<Phase, String> {
    let mut name = None;
    let mut tenant = "a".to_string();
    let (mut concurrency, mut requests, mut handler_ms, mut jitter_ms) = (None, None, 0, 0);
    let (mut idle, mut pause, mut wave) = (None, None, None);
    for part in spec.split(',').filter(|p| !p.is_empty()) {
        let (k, v) = part
            .split_once('=')
            .ok_or_else(|| format!("`{part}` is not key=value in phase `{spec}`"))?;
        let num = || {
            v.parse::<u64>()
                .map_err(|_| format!("`{k}` must be a non-negative integer in `{spec}`"))
        };
        match k {
            "name" => name = Some(v.to_string()),
            "tenant" => tenant = v.to_string(),
            "concurrency" => concurrency = Some(num()? as u32),
            "requests" => requests = Some(num()?),
            "handler_ms" => handler_ms = num()?,
            "jitter_ms" => jitter_ms = num()?,
            "idle_until_zero_ms" => idle = Some(num()?),
            "pause_ms" => pause = Some(num()?),
            "wave" => wave = Some(num()? as u32),
            other => return Err(format!("unknown phase key `{other}` in `{spec}`")),
        }
    }
    let name = name.ok_or_else(|| format!("phase `{spec}` needs name="))?;
    let kind = match (concurrency, requests, idle, pause) {
        (Some(c), Some(r), None, None) if c > 0 && r > 0 => PhaseKind::Invoke {
            tenant,
            concurrency: c,
            requests: r,
            handler_ms,
            jitter_ms,
        },
        (None, None, Some(timeout_ms), None) => PhaseKind::IdleUntilZero { tenant, timeout_ms },
        (None, None, None, Some(ms)) => PhaseKind::Pause { ms },
        _ => {
            return Err(format!(
                "phase `{spec}` needs concurrency>0 and requests>0, or idle_until_zero_ms, or pause_ms"
            ));
        }
    };
    Ok(Phase { name, wave, kind })
}

/// Refuse a plan that would exceed the declared limits before sending
/// anything. `already_sent` counts earlier runs of the same scenario.
pub fn check_plan(phases: &[Phase], limits: &LoadLimits, already_sent: u64) -> Result<(), String> {
    limits.validate()?;
    let mut total = already_sent;
    let mut i = 0;
    while i < phases.len() {
        // A wave's phases run at once: their concurrency adds up.
        let wave = phases[i].wave;
        let mut j = i + 1;
        while wave.is_some() && j < phases.len() && phases[j].wave == wave {
            j += 1;
        }
        let mut concurrent = 0;
        for p in &phases[i..j] {
            if let PhaseKind::Invoke {
                concurrency,
                requests,
                ..
            } = &p.kind
            {
                concurrent += concurrency;
                total += requests;
            }
        }
        if concurrent > limits.max_concurrency {
            return Err(format!(
                "phase `{}` runs {concurrent} workers at once, over the declared max_concurrency {}",
                phases[i].name, limits.max_concurrency
            ));
        }
        i = j;
    }
    if total > limits.max_requests {
        return Err(format!(
            "the plan sends {total} requests in this scenario, over the declared max_requests {}",
            limits.max_requests
        ));
    }
    Ok(())
}

/// xorshift64*: deterministic jitter from the recorded seed (no dependency).
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // splitmix64 of the seed: neighbouring seeds give unrelated streams,
        // and the state is never 0.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Self((z ^ (z >> 31)).max(1))
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    /// Uniform in `0..bound` (0 when `bound` is 0).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> LoadLimits {
        LoadLimits {
            max_concurrency: 8,
            max_requests: 50,
            max_duration_seconds: 60,
        }
    }

    #[test]
    fn phases_parse_and_plans_are_checked_against_the_declared_limits() {
        let burst =
            parse_phase("name=burst,tenant=b,concurrency=8,requests=24,handler_ms=400").unwrap();
        assert_eq!(
            burst.kind,
            PhaseKind::Invoke {
                tenant: "b".into(),
                concurrency: 8,
                requests: 24,
                handler_ms: 400,
                jitter_ms: 0
            }
        );
        let idle = parse_phase("name=idle,idle_until_zero_ms=30000").unwrap();
        assert!(matches!(
            idle.kind,
            PhaseKind::IdleUntilZero {
                timeout_ms: 30000,
                ..
            }
        ));
        assert!(parse_phase("name=x,concurrency=1").is_err());
        assert!(parse_phase("name=x,concurrency=0,requests=1").is_err());
        assert!(parse_phase("name=x,bogus=1").is_err());
        assert!(parse_phase("concurrency=1,requests=1").is_err());

        assert!(check_plan(std::slice::from_ref(&burst), &limits(), 0).is_ok());
        assert!(
            check_plan(std::slice::from_ref(&burst), &limits(), 30).is_err(),
            "scenario-wide"
        );
        let wide = parse_phase("name=w,concurrency=9,requests=9").unwrap();
        assert!(check_plan(&[wide], &limits(), 0).is_err());
        let long = parse_phase("name=l,tenant=a,concurrency=5,requests=5,wave=1").unwrap();
        let short = parse_phase("name=s,tenant=b,concurrency=4,requests=5,wave=1").unwrap();
        assert!(
            check_plan(&[long.clone(), short.clone()], &limits(), 0).is_err(),
            "a wave's workers add up"
        );
        let short_later = Phase {
            wave: Some(2),
            ..short
        };
        assert!(check_plan(&[long, short_later], &limits(), 0).is_ok());
        let over_ceiling = LoadLimits {
            max_concurrency: 1000,
            ..limits()
        };
        assert!(check_plan(&[], &over_ceiling, 0).is_err());
    }

    #[test]
    fn the_seed_makes_the_jitter_reproducible() {
        let draw = |seed| {
            let mut r = Rng::new(seed);
            (0..5).map(|_| r.below(100)).collect::<Vec<_>>()
        };
        assert_eq!(draw(42), draw(42));
        assert_ne!(draw(42), draw(43));
        assert!(draw(7).iter().all(|v| *v < 100));
        assert_eq!(Rng::new(1).below(0), 0);
    }
}
