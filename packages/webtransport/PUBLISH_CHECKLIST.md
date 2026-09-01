# Publish Verification Checklist

This fork is published to **GitHub Packages** (`https://npm.pkg.github.com`) as
`@lillious-networks/webtransport`. Publishing runs in CI via the `publish`
workflow (`workflow_dispatch`, input = release tag), which downloads the
prebuilt `.node` binaries from the matching GitHub Release, builds `dist/`, and
publishes to GitHub Packages using the built-in `GITHUB_TOKEN`.

Consumers install with a `.npmrc` pointing the `@lillious-networks` scope at
`https://npm.pkg.github.com` plus a token with `read:packages`.

## Before tagging a release

- [ ] `packages/webtransport/package.json` `version` bumped (semver).
- [ ] `dist/` builds clean:
  ```bash
  cd packages/webtransport && bun run clean && bun run build
  test -f dist/index.js && test -f dist/index.d.ts
  ```
- [ ] Native addon builds locally: `bun run build:native` (from repo root)
- [ ] Tests pass: `bun test`

## Release + publish

1. Commit, tag `vX.Y.Z` (must equal the package version), push:
   ```bash
   git tag vX.Y.Z && git push origin main --tags
   ```
2. The `release` workflow builds prebuilds for all targets
   (linux-x64, darwin-arm64, darwin-x64, win32-x64-msvc) and attaches them,
   plus `SHA256SUMS`, to a GitHub Release.
3. Run the `publish` workflow manually with the tag as input. It verifies
   checksums, builds `dist/`, and publishes to GitHub Packages.

## Verify the published package

```bash
# with GITHUB_TOKEN (read:packages) exported and a scoped .npmrc:
mkdir /tmp/wt-test && cd /tmp/wt-test && bun init -y
bun add @lillious-networks/webtransport@X.Y.Z
bun -e "import('@lillious-networks/webtransport').then(m=>console.log('OK', Object.keys(m).length))"
```

Confirm the native addon loads on your OS/arch (`win32-x64`, `linux-x64`,
`darwin-arm64`, `darwin-x64`). On unsupported platforms install may succeed but
import fails with a native-addon diagnostics message.
