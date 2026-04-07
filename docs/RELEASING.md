# Releasing TubeSift

This repository publishes release binaries and updates Homebrew automatically.

## One-time setup

1. In the main TubeSift repository settings, create secret:
   - `HOMEBREW_TAP_GITHUB_TOKEN` with `contents:write` access to `balyakin/homebrew-tubesift`.
2. Ensure the tap repo exists and is public:
   - `https://github.com/balyakin/homebrew-tubesift`

## Release flow

1. Update `project.version` in `pyproject.toml`.
2. Commit and push to `main`.
3. Create and push a tag in `vX.Y.Z` format:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## What happens automatically

The `release` workflow will:

1. Run tests and verify that tag version matches `pyproject.toml`.
2. Build single-file binaries via PyInstaller for:
   - macOS arm64
   - macOS x86_64
   - Linux x86_64
   - Windows x86_64
3. Create a GitHub Release and upload all artifacts + `SHA256SUMS.txt`.
4. Regenerate `Formula/tubesift.rb` in `balyakin/homebrew-tubesift` and push it.

## User install commands

```bash
brew tap balyakin/tubesift
brew install tubesift
```
