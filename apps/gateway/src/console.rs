//! `[console]` (PLT-4644): serve the built Functions console (`apps/console`)
//! as static files under `/console/`.
//!
//! The console is a client of the public management API only: it holds no
//! credential of its own, every request it makes carries the viewer's tenant
//! token and goes through the same `/v1` authentication and tenant checks as
//! the CLI. Serving it therefore adds no privilege; it only adds files. It is
//! off by default and the section is read by the gateway itself, so the
//! application configuration (`GatewayConfig`) is unchanged.
//!
//! ```toml
//! [console]
//! enabled = true
//! dir = "apps/console/out"   # the static export; relative to the working directory
//! ```

use std::path::{Path, PathBuf};

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{Redirect, Response};
use axum::routing::get;
use serde::Deserialize;
use tower_http::services::{ServeDir, ServeFile};

/// URL prefix the console is mounted at. The console build uses the same
/// `basePath`.
pub const MOUNT: &str = "/console";

/// Default location of the static export, relative to the working directory.
pub const DEFAULT_DIR: &str = "apps/console/out";

/// Content-Security-Policy of every console response: scripts, styles and
/// API calls only from this origin, no framing, no plugins.
pub const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; \
     style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; \
     connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; \
     frame-ancestors 'none'";

/// `[console]` of the gateway configuration file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsoleConfig {
    /// Serve the console under `/console/`. Default false.
    pub enabled: bool,
    /// Directory of the static export (must contain `index.html`). Default
    /// [`DEFAULT_DIR`]; a relative path is resolved against the working
    /// directory, like the other paths of the configuration.
    pub dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct Wrapper {
    #[serde(default)]
    console: ConsoleConfig,
}

impl ConsoleConfig {
    /// Read `[console]` from the text of a gateway configuration file. A file
    /// without the section yields the default (disabled).
    pub fn from_toml(text: &str) -> Result<Self, String> {
        // Other sections are `GatewayConfig`'s business; only `[console]` is
        // decoded here (serde ignores the unknown top-level keys).
        let wrapper: Wrapper = toml::from_str(text).map_err(|e| format!("[console]: {e}"))?;
        wrapper.console.resolve()
    }

    /// Load `[console]` from a gateway configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::from_toml(&text)
    }

    fn resolve(mut self) -> Result<Self, String> {
        if self.dir.as_ref().is_some_and(|d| d.as_os_str().is_empty()) {
            return Err("[console] dir must not be empty".into());
        }
        if self.enabled {
            let dir = self
                .dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DIR));
            let dir = if dir.is_absolute() {
                dir
            } else {
                std::env::current_dir()
                    .map_err(|e| format!("[console] cannot resolve dir: {e}"))?
                    .join(dir)
            };
            self.dir = Some(dir);
        }
        Ok(self)
    }

    /// The directory served when enabled.
    pub fn root(&self) -> PathBuf {
        self.dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DIR))
    }

    /// Enabled consoles must point at a built export: fail at startup rather
    /// than answer 404 for every page.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let root = self.root();
        if !root.join("index.html").is_file() {
            return Err(format!(
                "[console] enabled but {} has no index.html; build it with \
                 `pnpm --dir apps/console build` or set [console] enabled = false",
                root.display()
            ));
        }
        Ok(())
    }
}

/// Routes of the console: `GET /console` redirects to `/console/`, and
/// `/console/*` serves the export. An unknown path under `/console/` answers
/// the export's `404.html` with 404 (never an API error body, never a file
/// outside the root: `ServeDir` refuses `..` and absolute components).
///
/// Returns an empty router when the console is disabled, so `/console`
/// falls through to the gateway's JSON 404.
pub fn routes<S>(config: &ConsoleConfig) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if !config.enabled {
        return Router::new();
    }
    let root = config.root();
    let not_found = ServeFile::new(root.join("404.html"));
    let files = ServeDir::new(&root)
        .append_index_html_on_directories(true)
        .not_found_service(not_found);
    Router::new()
        .route(MOUNT, get(|| async { Redirect::permanent("/console/") }))
        .nest_service(&format!("{MOUNT}/"), files)
        .layer(axum::middleware::from_fn(security_headers))
}

/// Security headers on every console response, and `no-store` on documents
/// so a new build is picked up at once (the hashed `_next/static` assets can
/// be cached).
async fn security_headers(req: Request, next: Next) -> Response {
    let immutable = req.uri().path().starts_with("/console/_next/static/");
    let mut res = next.run(req).await;
    let h = res.headers_mut();
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if immutable {
            "public, max-age=31536000, immutable"
        } else {
            "no-store"
        }),
    );
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_section_is_disabled() {
        let c =
            ConsoleConfig::from_toml("listen = \"127.0.0.1:0\"\n[provider]\nkind = \"process\"\n")
                .unwrap();
        assert!(!c.enabled);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn unknown_console_keys_are_refused() {
        let err = ConsoleConfig::from_toml("[console]\nenabled = true\nport = 1\n").unwrap_err();
        assert!(err.contains("port"), "{err}");
    }

    #[test]
    fn enabled_without_a_build_fails_validation() {
        let dir = tempfile::tempdir().unwrap();
        let text = format!(
            "[console]\nenabled = true\ndir = \"{}\"\n",
            dir.path().display()
        );
        let c = ConsoleConfig::from_toml(&text).unwrap();
        let err = c.validate().unwrap_err();
        assert!(err.contains("index.html"), "{err}");
        std::fs::write(dir.path().join("index.html"), "<html></html>").unwrap();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn relative_dir_is_resolved_against_the_working_directory() {
        let c = ConsoleConfig::from_toml("[console]\nenabled = true\n").unwrap();
        let root = c.root();
        assert!(
            root.is_absolute() && root.ends_with(DEFAULT_DIR),
            "{root:?}"
        );
    }
}
