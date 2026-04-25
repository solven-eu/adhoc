# Pivotable on Render (Docker runtime)

## One-time setup (~5 min)

1. Create a [Render](https://render.com) account, connect your Github.
2. Dashboard → **New** → **Blueprint**, point at the repo.
3. Render reads `docs/deploy/render/render.yaml` and proposes a `web` service named `pivotable`. Accept.
4. Wait for the first build (~3-5 min, dominated by Maven dependency download; subsequent builds hit the Docker layer cache and land in ~30-60 s).
5. The service URL is `https://pivotable-XXXX.onrender.com`.

## What's deployed

- `pivotable-server-webflux` — Spring Boot 4 on Netty, Java 25.
- Active profiles: `pivotable-unsafe,pivotable-simple_datasets` — in-memory fake-user login + example cubes, zero external dependencies.
- Login: click **pivotable-unsafe_fakeuser** → **Login fakeUser**.

## Cold starts

The `free` plan sleeps the service after 15 min of inactivity. First hit after sleep pays ~10-20 s (container wake + Spring Boot startup). Subsequent requests land in tens of milliseconds. If that penalty is a problem, bump `plan: free` → `plan: starter` in `render.yaml` (currently $7/mo, always-on).

## Switching to real OAuth2

The blueprint's `envVars` section includes commented-out entries for GitHub OAuth2. Uncomment them, then in the Render dashboard → service → Environment, enter `ADHOC_PIVOTABLE_LOGIN_OAUTH2_GITHUB_CLIENTID` and `ADHOC_PIVOTABLE_LOGIN_OAUTH2_GITHUB_CLIENTSECRET`. Also update the GitHub OAuth app's redirect URI to `https://pivotable-XXXX.onrender.com/login/oauth2/code/github`.

## Troubleshooting

- **Build times out**: rare on Maven Central; the `MAVEN_OPTS=-Dmaven.wagon.http.pool=false` env var in the blueprint usually fixes it. A rebuild via the dashboard's **Clear build cache & deploy** is the next step.
- **502 on first request after deploy**: Spring Boot is still starting. Wait 15 s and retry. Render's TCP-port healthcheck may report ready before the HTTP stack is.
- **Out of memory**: the `free` plan is capped at 512 MB. The Dockerfile's `MaxRAMPercentage=75` leaves headroom, but if you load big datasets via `pivotable-advanced_datasets`, upgrade the plan.

