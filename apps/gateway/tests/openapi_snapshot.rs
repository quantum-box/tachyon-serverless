//! OpenAPI contract snapshot (PLT-4645).
//!
//! The document the gateway serves at `/openapi.json` is generated from the
//! utoipa annotations. A change to a handler signature, a schema, a status code
//! or a path changes the public contract, so it must show up in review as a
//! diff of the committed `docs/openapi.json`, not only as a code change.
//!
//! This test fails when the generated document differs from the snapshot. To
//! accept an intended change, regenerate the snapshot and commit it:
//!
//! ```text
//! TSLS_UPDATE_SNAPSHOTS=1 cargo test -p tachyon-serverless-gateway --test openapi_snapshot
//! ```

use std::path::PathBuf;

use serde_json::Value;

fn snapshot_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/openapi.json")
}

/// Pretty JSON with sorted object keys and a trailing newline, so the snapshot
/// is stable regardless of serde_json's `preserve_order` feature.
fn render(value: &Value) -> String {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for k in keys {
                    out.insert(k.clone(), sorted(&map[k]));
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    let mut s = serde_json::to_string_pretty(&sorted(value)).expect("serialize openapi");
    s.push('\n');
    s
}

/// JSON pointers at which `a` and `b` differ (at most `limit`).
fn differences(a: &Value, b: &Value, pointer: String, out: &mut Vec<String>, limit: usize) {
    if out.len() >= limit || a == b {
        return;
    }
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                let p = format!("{pointer}/{}", k.replace('~', "~0").replace('/', "~1"));
                match (x.get(k), y.get(k)) {
                    (Some(l), Some(r)) => differences(l, r, p, out, limit),
                    (Some(_), None) => out.push(format!("{p}: only in the snapshot")),
                    (None, Some(_)) => out.push(format!("{p}: only in the generated document")),
                    (None, None) => unreachable!(),
                }
            }
        }
        (Value::Array(x), Value::Array(y)) if x.len() == y.len() => {
            for (i, (l, r)) in x.iter().zip(y).enumerate() {
                differences(l, r, format!("{pointer}/{i}"), out, limit);
            }
        }
        _ => out.push(format!("{pointer}: snapshot {a} != generated {b}")),
    }
}

#[test]
fn openapi_document_matches_the_committed_snapshot() {
    let generated = tachyon_serverless_gateway::openapi::document();
    assert!(
        generated.get("error").is_none(),
        "the OpenAPI document failed to serialize: {generated}"
    );
    let path = snapshot_path();

    if std::env::var_os("TSLS_UPDATE_SNAPSHOTS").is_some() {
        std::fs::write(&path, render(&generated)).expect("write docs/openapi.json");
        eprintln!("updated {}", path.display());
        return;
    }

    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {} ({e}). Generate it with \
             `TSLS_UPDATE_SNAPSHOTS=1 cargo test -p tachyon-serverless-gateway --test openapi_snapshot`",
            path.display()
        )
    });
    let snapshot: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()));

    let mut diffs = Vec::new();
    differences(&snapshot, &generated, String::new(), &mut diffs, 25);
    assert!(
        diffs.is_empty(),
        "the OpenAPI contract changed but docs/openapi.json was not updated.\n\
         Differences (JSON pointer):\n  {}\n\
         If the change is intended, run \
         `TSLS_UPDATE_SNAPSHOTS=1 cargo test -p tachyon-serverless-gateway --test openapi_snapshot` \
         and commit docs/openapi.json so the contract change is reviewed.",
        diffs.join("\n  ")
    );
    // The committed file must also be in canonical form, so that regenerating it
    // never produces an unrelated whitespace-only diff.
    assert_eq!(
        text,
        render(&snapshot),
        "docs/openapi.json is not in canonical form; regenerate it with TSLS_UPDATE_SNAPSHOTS=1"
    );
}

#[test]
fn every_management_route_requires_the_bearer_scheme() {
    // Contract guard next to the snapshot: a new `/v1/` operation without
    // `security(("bearer" = []))` would document an unauthenticated endpoint.
    let doc = tachyon_serverless_gateway::openapi::document();
    let paths = doc["paths"].as_object().expect("paths");
    let mut unauthenticated = Vec::new();
    for (path, item) in paths {
        if !path.starts_with("/v1/") {
            continue;
        }
        for (method, op) in item.as_object().expect("path item") {
            let secured = op["security"]
                .as_array()
                .is_some_and(|s| s.iter().any(|req| req.get("bearer").is_some()));
            if !secured {
                unauthenticated.push(format!("{method} {path}"));
            }
        }
    }
    assert!(
        unauthenticated.is_empty(),
        "operations without bearer security: {unauthenticated:?}"
    );
}
