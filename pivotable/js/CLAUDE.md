# pivotable/js — Local AI agent rules

Scoped guidance for the Pivotable SPA (everything under `src/main/resources/static/ui/js/`,
`unit-tests/`, `e2e-tests/`). The root `CLAUDE.md` still applies; the rules below are the ones
that _bite specifically here_ and have been observed to be easy to forget.

## Never write a backtick inside a Vue template literal

Vue components in this codebase use the pattern:

```js
export default {
	template: /* HTML */ ` <div>…HTML…</div> `,
};
```

The backticks delimit a **JavaScript template literal**. A backtick character anywhere inside —
even inside an `<!-- HTML comment -->` or a JSDoc-style block — terminates the string mid-way and
breaks the file. The downstream symptom is a confusing prettier/biome parse error
(`LINE_UNDEFINED`, `Code formatting aborted due to parsing errors`) that does **not** point at
the real location.

The most common slip is using backticks as the "code fragment" delimiter in prose:

❌ Don't — every backtick after the first one terminates the template literal:

```js
template: /* HTML */ `
	<!-- The `btn-link` class strips chrome so it still looks passive. -->
	<button class="btn btn-link">…</button>
`,
```

✅ Do — use plain prose, single-quotes, or `&#96;` for the rare case you need a literal backtick:

```js
template: /* HTML */ `
	<!-- The btn-link class strips chrome so it still looks passive. -->
	<button class="btn btn-link">…</button>
`,
```

Same rule for `${foo}` interpolation syntax — it must not appear inside the template, even in
comments. Use plain prose.

Sanity check before saving any file containing a Vue `template: /* HTML */ \`…\``: scan from the
opening backtick to the closing backtick for any *other* backtick or `${`. Find any → rewrite.

## TypeScript-via-JSDoc gate

Every new `.js` file under this tree starts with `// @ts-check`. CI runs `tsc --noEmit`. Use
`/** @type {…} */` casts to escape narrow typings rather than disabling the check.

## Formatting

After every JS edit, run (from this directory):

```
npm run format && mvn spotless:apply
```

The order matters: prettier first, spotless second. Prettier and Biome's Spotless integration
use different line-wrap algorithms for expressions over 160 cols; running prettier _after_
spotless can re-introduce rewrites that spotless would normalise on the next pass.

## Vitest specs avoid the DOM

The standard `vitest` run uses Node without a DOM. Specs that import a Vue component
transitively pull `bootstrap` (or `mermaid`, or any DOM-touching dependency) and fail with
`document is not defined` _before any test runs_.

If a component's logic is unit-testable, **extract the pure logic to a sibling file** that the
spec imports directly. The component still composes it; the spec doesn't drag the DOM in.

Example:

- `adhoc-query-plan-poll.js` — pure `fetchSummary` + `nextPollState`.
- `adhoc-query-plan-live.js` — Vue component that imports the pure helpers AND the Bootstrap
  modal.
- `unit-tests/query-plan-live.spec.js` — imports from `adhoc-query-plan-poll.js`, never touches
  the component.

## Dev stack

Two processes — Spring Boot on `:8080`, Vite on `:5173` (proxies `/api`, `/webjars`, `/login`,
`/logout`, `/oauth2` to the backend). All commands run from this directory:

|        Command         |                                What it starts                                 |
|------------------------|-------------------------------------------------------------------------------|
| `npm run backend`      | Spring Boot only (`mvn spring-boot:run`, default profile `pivotable-unsafe`). |
| `npm run dev_frontend` | Vite only — assumes a backend is already running on `:8080`.                  |
| `npm run dev_stack`    | Both — Ctrl-C kills both.                                                     |

`npx playwright test` implicitly invokes `npm run backend` if nothing is on `:8080`.

When the user reports a problem visible at `http://localhost:5173/...` and the SPA route already
exists, the most likely cause is **stale bytecode** on the running JVM — restart the backend so
fresh code is compiled. The full contributing guide is in
[`../CONTRIBUTING.md`](../CONTRIBUTING.md) (profiles, login flow, importmap modes).

## When async UX appears in the UI, surface it

Spinner / progress / status text alongside every async action — see the project-wide UX rule in
the root `CLAUDE.md`. The local twist: most components in this tree already follow the pattern
(`.spinner-border-sm` next to a label, `.progress-bar-animated` for longer ops); copy a sibling's
markup rather than inventing a new idiom.
