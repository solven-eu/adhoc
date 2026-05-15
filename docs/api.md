# Pivotable HTTP API conventions

This page documents the cross-cutting conventions every endpoint under `/api/v1` follows. It is the
contract a third-party client (CLI, MCP bridge, custom UI) can assume; the SPA itself relies on it
implicitly. For per-endpoint shape (request body, response body), consult the OpenAPI surface
exposed at `/swagger-ui` (when `springdoc` is on the classpath) or the Java controllers / handlers.

## HTTP status codes

|            Status             |                                                                                                                                                                                                                                                                                                                                           Meaning                                                                                                                                                                                                                                                                                                                                            |
|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **200 OK**                    | The request succeeded and the body carries the requested representation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| **204 No Content**            | The endpoint accepted the request but has nothing to return. May be temporal (the underlying state is not yet available — see [Retry-After](#retry-after-temporal-vs-terminal-204)) or terminal (the underlying resource is genuinely absent — typo / stale link / evicted / completed without a representation).                                                                                                                                                                                                                                                                                                                                                                            |
| **400 Bad Request**           | The request shape is malformed (bad JSON, missing required field, unparseable UUID).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **401 Unauthorized**          | The request has no valid bearer token. The SPA opens the login modal on this status.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **403 Forbidden**             | The bearer token is valid but the account does not have access to the requested resource.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **404 Not Found**             | **Reserved for "the endpoint itself does not exist".** Hitting an unknown path — e.g. a typo in the URL or a removed endpoint — returns 404. **A known endpoint never returns 404 because the underlying resource is absent;** it returns 204 instead. This is a deliberate departure from the common REST convention where 404 doubles as "this id is unknown". The rationale is the SPA's polling loop: a 404 unambiguously says "give up, this URL is wrong", while a 204 says "the URL is right, the resource is just not here right now (possibly yet)". Conflating the two would force every polling client to special-case "the URL was right yesterday, did you misroute me today?". |
| **500 Internal Server Error** | An unexpected exception bubbled up from the controller. The response body is `{"error_message": "..."}` (see [`PivotableWebfluxExceptionHandler`](https://github.com/solven-eu/adhoc/blob/master/pivotable/server-webflux/src/main/java/eu/solven/adhoc/pivotable/webflux/PivotableWebfluxExceptionHandler.java) / [`PivotableWebmvcExceptionHandler`](https://github.com/solven-eu/adhoc/blob/master/pivotable/server-webmvc/src/main/java/eu/solven/adhoc/pivotable/webmvc/PivotableWebmvcExceptionHandler.java)).                                                                                                                                                                         |

### Retry-After: temporal vs terminal 204

A 204 may be **temporal** (the resource will become available later — keep polling) or **terminal**
(the resource will never be available — stop). The two are disambiguated by the `Retry-After`
response header:

|              Status + headers               |                   Meaning                    |         Client behaviour         |
|---------------------------------------------|----------------------------------------------|----------------------------------|
| `204 No Content` + `Retry-After: <seconds>` | Temporal. The server is working on it.       | Wait the indicated delay, retry. |
| `204 No Content` (no `Retry-After`)         | Terminal. There is nothing more to wait for. | Stop polling.                    |

Clients should treat the absence of `Retry-After` on a 204 as a hard "give up" — re-issuing the
request would yield another 204 indefinitely.

## Query plan endpoints

The plan registry is the only Pivotable subsystem that currently uses the temporal/terminal 204
contract end-to-end. The path family is `/api/v1/cubes/queries/{queryUuid}/plan/*`, where
`{queryUuid}` is the UUID returned by `POST /api/v1/cubes/query/asynchronous`.

### `GET .../plan/summary`

Cheap status object suitable for high-frequency polling (UI Live View badge). Counts of nodes by
state, elapsed/wait time, last finished step.

|                                      State                                      | Status |     Headers      |          Body           |                        SPA renders                         |
|---------------------------------------------------------------------------------|--------|------------------|-------------------------|------------------------------------------------------------|
| Engine has registered a plan                                                    | 200    | —                | `QueryPlanSummary` JSON | `Queued` / `Running` / `Done` / `Failed` badge with counts |
| Pivotable accepted the submission, engine is still planning                     | 204    | `Retry-After: 1` | empty                   | `Queuing…` + spinner                                       |
| No plan can be served for this UUID (unknown UUID, evicted post-termination, …) | 204    | —                | empty                   | `No plan available` hint, polling stops                    |

### `GET .../plan/snapshot`

Full plan tree. Pay this once when the user opens the detail modal; keep polling the cheap summary
for the badge. Same 200/204/204 contract as `/plan/summary`.

### `GET .../plan/children`

Composite-cube fan-out: returns one `QueryPlanSummary` per registered sub-cube plan whose parent
matches `{queryUuid}`. Empty array on 200 for a non-composite query (the normal case). 204 with
`Retry-After` while the parent plan is still being prepared; 204 without `Retry-After` for an
unknown parent UUID.

## Async-query endpoints

Submission + result-polling for long-running queries. Submission returns the UUID immediately; the
SPA then polls `/result` until the query completes.

### `POST /api/v1/cubes/query/asynchronous`

Body: `TargetedCubeQuery` JSON. Returns the UUID Pivotable + the engine adopted for this submission
(see `SubmittedQueryIdScope` — the same UUID is used as `AdhocQueryId.queryId` inside the engine,
so it can be joined against the plan registry without a separate mapping).

### `GET /api/v1/cubes/query/result?query_id=<uuid>&with_view=<bool>`

Polls the async-query manager. Always returns 200 with a `QueryResultHolder` body — distinct states
distinguish the lifecycle without needing distinct HTTP statuses:

|          Manager state           |                                         Body fields                                         |
|----------------------------------|---------------------------------------------------------------------------------------------|
| `RUNNING`                        | `state: "RUNNING"`, `retryIn` / `retryInMs` (exponential backoff hint)                      |
| `SERVED` (and `with_view=true`)  | `state: "SERVED"`, `view: ListBasedTabularView`                                             |
| `SERVED` (and `with_view=false`) | `state: "SERVED"` only — caller fetches the view separately                                 |
| `FAILED`                         | `state: "FAILED"`, `errorMessage` (first stack line), `stacktrace` (full server-side trace) |
| `DISCARDED` / `UNKNOWN`          | `state: "DISCARDED"` or `"UNKNOWN"` (no other fields)                                       |

Errors raised during execution surface here, **not** through the global `…ExceptionHandler` — the
failure happened inside a background future, not at the controller layer. The SPA reads
`errorMessage` and `stacktrace` to render the "Query is broken" banner with a collapsible stack
trace.
