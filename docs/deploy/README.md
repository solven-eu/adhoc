# Deploying Pivotable

Pivotable is a self-contained Spring Boot application. With the `pivotable-simple_datasets` profile it loads example data in-memory and needs no external database or CDN — any host that can run a Java 25 process and forward one HTTP port will do.

Two recipes live alongside this file; pick the one that matches your constraints.

|            Recipe            | Subdirectory |          Cost           |          Uptime          | Setup time |                                              Notes                                               |
|------------------------------|--------------|-------------------------|--------------------------|------------|--------------------------------------------------------------------------------------------------|
| **Render** (Docker)          | `render/`    | Free tier (web service) | Sleeps after 15 min idle | ~5 min     | Cold-start penalty of ~10-20 s on the first hit; after that, no-ops. Auto-deploys on `git push`. |
| **Oracle Cloud Always-Free** | `oracle/`    | Free forever            | 24/7                     | ~1 h       | ARM Ampere A1-Flex VM + systemd. More ops, no cold starts, permanent URL.                        |

Both recipes run **`pivotable-server-webflux`** with profiles `pivotable-unsafe,pivotable-simple_datasets`:
- `pivotable-unsafe` enables the in-memory fake-user login (no real OAuth2 credentials needed).
- `pivotable-simple_datasets` registers the example cubes (`simple`, `ban`, `films`, `people`, `pixar`, `WorldCupPlayers`).

If you later need real GitHub/Google OAuth2, swap `pivotable-unsafe` for `pivotable-unsafe_external_oauth2` and pass `ADHOC_PIVOTABLE_LOGIN_OAUTH2_GITHUB_CLIENTID` / `CLIENTSECRET` as environment variables — the profile's YAML already declares those placeholders.
