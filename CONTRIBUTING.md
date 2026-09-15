# Contributing Guide

tachyon-serverless へのコントリビュートに興味を持っていただきありがとうございます。

## Issue

- バグ報告・機能提案は [Issue テンプレート](https://github.com/quantum-box/tachyon-serverless/issues/new/choose) から作成してください。
- 既存の Issue に同じ内容がないか、事前に検索してください。
- セキュリティに関する問題は Issue にせず、[SECURITY.md](SECURITY.md) の手順で報告してください。

## 開発環境と品質ゲート

Rust 1.95.0（`rust-toolchain.toml` / `mise.toml` で固定）。`mise install` で揃います。

```sh
cargo build --workspace                                    # ビルド
cargo test --workspace                                     # テスト
cargo fmt --all -- --check                                 # フォーマット
cargo clippy --workspace --all-targets -- -D warnings      # lint（警告ゼロ）

# guest 側（Firecracker に載せる static バイナリ）のビルド確認
cargo build --release --target x86_64-unknown-linux-musl \
  -p tachyon-serverless-runtime-bridge -p example-hello -p example-http-axum -p example-cpu-burn

# シェルスクリプト
shellcheck -x -P scripts/e2e scripts/e2e/*.sh
scripts/e2e/selftest.sh                                    # e2e ヘルパの自己テスト
scripts/e2e/demo.sh                                        # E2E デモ（process provider）
```

CI（`.github/workflows/ci.yml`）は上記と同じコマンドを実行します。PR を出す前にローカルで通してください。
契約 crate（`crates/domain` `crates/protocol` `crates/provider-port` `crates/api-types`）の public API を変える場合は、理由を PR に書いてください。

## Pull Request

1. リポジトリを fork し、`main` からブランチを作成します（例: `feat/xxx`, `fix/xxx`）。
2. 変更を加え、必要に応じてテストとドキュメントを更新します。
3. コミットメッセージは [Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/) 形式を推奨します。
   - `feat: ...` / `fix: ...` / `docs: ...` / `refactor: ...` / `test: ...` / `chore: ...`
4. Pull Request を作成し、テンプレートに沿って変更内容を記載してください。
5. 大きな変更は、実装前に Issue で方針を相談してください。

## ライセンス

このリポジトリへのコントリビュートは、[MIT License](LICENSE) の下で提供されることに同意したものとみなされます。
