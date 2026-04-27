---

description: Start (or restart) the Pivotable dev stack — Spring Boot backend on :8080 + Vite frontend on :5173 with HMR. Idempotent: kills any process holding either port first.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Start or restart the local Pivotable dev stack so the user can interact with the Vue UI against a live backend.

The dev stack is two long-running processes started together:

- **Backend** on `:8080` — `mvn spring-boot:run` via `npm run backend` (default `WEBMODE=webflux`, profile `pivotable-unsafe`).
- **Frontend** on `:5173` — Vite dev server with HMR via `npm run dev_frontend`. Proxies `/api`, `/webjars`, `/login`, `/logout` to the backend.

Both are launched together by `npm run dev_stack` from `pivotable/js/`.

## Steps

1. **Kill any process already holding `:8080` or `:5173`.** Run `pkill -f "PivotableServer"`, `pkill -f "spring-boot:run"`, `pkill -f "vite"` (each `|| true` so a missing process doesn't fail the command), then `sleep 3` to let ports release.
2. **Confirm the ports are free** — `lsof -i :8080` and `lsof -i :5173` must both return empty.
3. **Launch the stack in the background** with `Bash` `run_in_background: true`:

```
cd /Users/blacelle/workspace4/adhoc/pivotable/js && nohup npm run dev_stack > /tmp/pivotable-dev-stack.log 2>&1 &
```

4. **Wait for both processes to be ready** using `Monitor` with this until-loop (one event when both are up):

```
until grep -q "Started PivotableServer" /tmp/pivotable-dev-stack.log && grep -qE "ready in [0-9]+ ms|Local:.*5173" /tmp/pivotable-dev-stack.log; do sleep 1; done; echo "READY"
```

Timeout 90 seconds. Backend startup is ~5 s, Vite ~1 s; the long pole is Maven dependency resolution on a cold cache.
5. **Report back**: confirm both ports are listening, surface the URLs:
- Backend: http://localhost:8080
- Frontend (with HMR): http://localhost:5173
- Login flow: see `CLAUDE.md` "Logging in to a dev stack" — fakeUser is pre-filled, 3 clicks.

## Notes

- **Don't start a foreground `Bash`** — the dev_stack never exits on its own; foreground would block the session. Use `run_in_background: true`.
- **The log file is `/tmp/pivotable-dev-stack.log`.** Tail it with `tail -f` (Monitor) for live troubleshooting.
- **Stopping the stack**: `pkill -f "PivotableServer" ; pkill -f "vite"`. The user's Ctrl-C in the terminal where they ran `npm run dev_stack` themselves does the same — but if I started it, I have to `pkill`.
- **Honor user environment**: if `WEBMODE` or `SPRING_ACTIVE_PROFILES` is already set in the user's shell and they want a different profile, surface that question before launching. Otherwise default to `webflux` + `pivotable-unsafe`.
- **After backend code edits**: a restart is required (Spring Boot devtools is not configured). Re-run this command — step 1 will kill the stale backend and step 3 will pick up the freshly-built classes from `target/classes`.
- **After frontend code edits**: Vite HMR picks them up automatically. No restart needed.

