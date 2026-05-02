# docs/ — Documentation Guidelines

## Reachability rule

Every `.md` file under `docs/` must be reachable from `index.md` in **at most 2 hops**:

- **1 hop** — the file is linked directly from `index.md`.
- **2 hops** — the file is linked from a page that is itself linked from `index.md`.

When adding a new `.md` file to `docs/`, add a link to it in `index.md` (preferred) or in a page already linked from `index.md`. Do not leave orphan files.

## No relative links going out of `docs/`

MkDocs runs in **strict mode** on CI and only sees files under `docs/`. A relative link that
escapes the tree (e.g. `../SECURITY.MD`, `../README.MD`) resolves to a file MkDocs cannot
resolve — the build fails with `Aborted with N warnings in strict mode!`.

When you need to link to a file that lives **outside** `docs/` (repo root files like
`SECURITY.MD`, `README.MD`, `CHANGES.MD`, or anything under `pivotable/`, `adhoc/`, etc.),
use the **absolute GitHub URL** on the `master` branch:

```markdown
[SECURITY.MD § EvalEx](https://github.com/solven-eu/adhoc/blob/master/SECURITY.MD#evalex--expression-evaluation-optional-dependency)
```

Do **not** use `../SECURITY.MD` or any `../` path. Relative links are reserved for
`docs/*.md` → `docs/*.md` navigation, which is what MkDocs can resolve.

## Bi-directional links

Links between articles should generally be **bi-directional**: if article A links to article B,
article B should link back to A. This keeps the documentation navigable from either direction and
makes relationships explicit.

When adding a new article that references existing ones, check each referenced article and add a
return link if it does not already have one.

## Moving files here from the project root

When a root-level `*.MD` file is moved into `docs/`:

1. Use `git mv` to preserve history.
2. Rename to lowercase `.md` to follow the existing convention.
3. Add the file to `index.md` so it satisfies the reachability rule.
4. Update any existing links in `README.MD` or other files to point to the new path.

