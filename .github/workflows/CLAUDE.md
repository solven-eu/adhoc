# .github/workflows — CI Guidelines

## Pin all versions

Every tool, action, and package version used in a workflow **must be pinned** to an exact version.
Do **not** use floating tags such as `latest`, `v3`, or `*`.

### GitHub Actions (`uses:`)

Pin to the full SHA of the commit, with the human-readable tag as a comment:

```yaml
- uses: actions/checkout@abc1234def5678...  # v4.1.1
```

Never write `uses: actions/checkout@v4` — that tag can be force-pushed.

### Python packages (`pip install`)

Always specify exact versions:

```yaml
- run: pip install mkdocs-material==9.7.5 "mkdocs==1.6.1"
```

Do **not** write `pip install mkdocs-material` or `pip install mkdocs-material>=9`.

### Node packages (`npm install` / `npx`)

Always specify exact versions in `package.json` (no `^` or `~` prefixes) and commit
`package-lock.json`. When invoking a package directly with `npx`, pin the version:

```yaml
- run: npx prettier@3.3.3 --check .
```

### Docker images

Always include the digest alongside the tag:

```yaml
image: python:3.12.4-slim@sha256:abc123...
```

## Why

Unpinned versions have caused broken CI in this repository (notably the MkDocs deployment
workflow broke when an upstream package released a new version). Pinning guarantees
reproducible builds and prevents supply-chain attacks via tag mutation.
