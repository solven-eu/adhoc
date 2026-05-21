// @ts-check
//
// Shared base URL for every spec under `e2e-tests/`. Reads `PIVOTABLE_BASE_URL` from the
// environment so a developer can point the suite at a non-default backend without editing
// any spec — e.g. when the default `:8080` slot is already occupied by another instance
// and the user is running their backend on `:8090`:
//
//   PIVOTABLE_BASE_URL=http://localhost:8090 npx playwright test --project=chromium
//
// Defaults to `http://localhost:8080`, matching the historical behaviour and the
// `webServer` recipe in `pivotable/CONTRIBUTING.md`.
//
// Exported as `BASE_URL` (not `URL`) to avoid shadowing the global `URL` constructor.

export const BASE_URL = process.env.PIVOTABLE_BASE_URL ?? "http://localhost:8080";
