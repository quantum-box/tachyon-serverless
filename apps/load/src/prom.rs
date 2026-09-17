//! A small reader of the Prometheus text exposition, enough for the gateway's
//! own `/metrics` (no exemplars, no timestamps).

use std::collections::BTreeMap;

/// `name{labels}` (labels sorted) -> value. Histogram buckets are dropped:
/// the sampler records counters and gauges only.
pub type Flat = BTreeMap<String, f64>;

/// One parsed sample.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

fn unescape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let mut chars = v.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_labels(body: &str) -> Option<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let eq = body[i..].find('=')? + i;
        let key = body[i..eq].trim_start_matches(',').trim().to_string();
        if bytes.get(eq + 1) != Some(&b'"') {
            return None;
        }
        let mut j = eq + 2;
        while j < bytes.len() {
            if bytes[j] == b'\\' {
                j += 2;
                continue;
            }
            if bytes[j] == b'"' {
                break;
            }
            j += 1;
        }
        if j >= bytes.len() {
            return None;
        }
        labels.insert(key, unescape(&body[eq + 2..j]));
        i = j + 1;
    }
    Some(labels)
}

/// Parse every sample line; malformed lines are skipped.
pub fn parse(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let value = match value {
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            v => match v.parse::<f64>() {
                Ok(v) => v,
                Err(_) => continue,
            },
        };
        let (name, labels) = match series.split_once('{') {
            None => (series.to_string(), BTreeMap::new()),
            Some((name, rest)) => match rest.strip_suffix('}').and_then(parse_labels) {
                Some(labels) => (name.to_string(), labels),
                None => continue,
            },
        };
        out.push(Sample {
            name,
            labels,
            value,
        });
    }
    out
}

/// The flat key of a series.
pub fn key(name: &str, labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let inner: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{name}{{{}}}", inner.join(","))
}

/// Counters and gauges of `text` as a flat map (histograms dropped).
pub fn flatten(text: &str) -> Flat {
    parse(text)
        .into_iter()
        .filter(|s| {
            !(s.name.ends_with("_bucket") || s.name.ends_with("_sum") || s.name.ends_with("_count"))
        })
        .map(|s| (key(&s.name, &s.labels), s.value))
        .collect()
}

/// Split a flat key back into name and labels.
pub fn split_key(key: &str) -> (&str, BTreeMap<String, String>) {
    match key.split_once('{') {
        None => (key, BTreeMap::new()),
        Some((name, rest)) => {
            let body = rest.strip_suffix('}').unwrap_or(rest);
            let labels = body
                .split(',')
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            (name, labels)
        }
    }
}

/// Every series of `family` in `flat` with its labels.
pub fn series<'a>(
    flat: &'a Flat,
    family: &'a str,
) -> impl Iterator<Item = (BTreeMap<String, String>, f64)> + 'a {
    flat.iter().filter_map(move |(k, v)| {
        let (name, labels) = split_key(k);
        (name == family).then_some((labels, *v))
    })
}

/// The value of an unlabelled series, or the sum over matching labels.
pub fn value(flat: &Flat, family: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let mut found = None;
    for (l, v) in series(flat, family) {
        if labels
            .iter()
            .all(|(k, want)| l.get(*k).map(String::as_str) == Some(*want))
        {
            *found.get_or_insert(0.0) += v;
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_labels_escapes_and_flattens_without_histograms() {
        let text = "# HELP x y\n# TYPE x gauge\n\
            tsls_environments{state=\"busy\"} 2\n\
            tsls_node_info{node=\"a\\\"b,c\"} 1\n\
            tsls_queue_length 0\n\
            tsls_attempt_phase_seconds_bucket{le=\"+Inf\",phase=\"total\"} 3\n\
            broken{ 1\n";
        let samples = parse(text);
        assert_eq!(samples.len(), 4);
        assert_eq!(samples[1].labels["node"], "a\"b,c");
        let flat = flatten(text);
        assert_eq!(flat.len(), 3);
        assert_eq!(
            value(&flat, "tsls_environments", &[("state", "busy")]),
            Some(2.0)
        );
        assert_eq!(value(&flat, "tsls_queue_length", &[]), Some(0.0));
        assert_eq!(value(&flat, "tsls_missing", &[]), None);
    }
}
