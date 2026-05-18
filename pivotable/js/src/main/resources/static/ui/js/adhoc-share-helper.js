// @ts-check

import queryHelper from "./adhoc-query-helper.js";

const SHARE_ROUTE = "/html/share";

/**
 * Build the shareable payload describing a Pivotable view: the endpoint it lives on, the cube it
 * targets, and the queryModel state.
 *
 * <p>Scope is intentionally minimum — endpoint coordinates + cubeId + query. Display preferences
 * (heatmaps, frozen columns, …) are NOT exported: they are user-level concerns, not part of the
 * view contract.
 *
 * <p>The endpoint is serialized as `{url, name}` — a single string url rather than separate
 * host/port/prefix — because (a) server-discovered endpoints in the adhoc store only carry
 * `{id, name, url}` (no structured host/port/prefix), and (b) the sentinel "self" endpoint url
 * (`http://localhost:self`) is non-numeric in its port and can't be re-assembled from parts.
 * Receivers parse the url back into host/port/prefix when they need to register it locally.
 *
 * @param {{ host?: string, port?: number, prefix?: string, name?: string, url?: string }} endpoint
 *     either a local-endpoint shape (host/port/prefix) or a server-discovered shape (url only)
 * @param {string} cubeId
 * @param {Object} queryModel
 * @returns {{ endpoint: { url: string, name: string }, cubeId: string, query: Object }}
 */
export function buildSharePayload(endpoint, cubeId, queryModel) {
	if (!endpoint) {
		throw new Error("endpoint is required");
	}
	const url = endpoint.url ? String(endpoint.url) : buildUrlFromParts(endpoint);
	if (!url) {
		throw new Error("endpoint must carry either `url` or `host`");
	}
	return {
		endpoint: {
			url,
			name: endpoint.name ? String(endpoint.name) : "",
		},
		cubeId: String(cubeId || ""),
		query: queryHelper.queryModelToParsedJson(queryModel),
	};
}

function buildUrlFromParts(endpoint) {
	if (!endpoint.host) return "";
	const port = Number(endpoint.port) || 8080;
	let prefix = endpoint.prefix ? String(endpoint.prefix) : "";
	if (prefix && !prefix.startsWith("/")) prefix = "/" + prefix;
	while (prefix.endsWith("/")) prefix = prefix.slice(0, -1);
	return "http://" + endpoint.host + ":" + port + prefix;
}

/**
 * Pretty-printed JSON form of a share payload. Stable indent for human inspection.
 *
 * @param {Object} payload
 * @returns {string}
 */
export function encodePayloadToJson(payload) {
	return JSON.stringify(payload, null, 2);
}

/**
 * URL form of a share payload — a deep link of shape `${origin}${SHARE_ROUTE}#<encoded-json>`.
 * The hash carries the entire payload so no server round-trip is involved on receive.
 *
 * @param {Object} payload
 * @param {string} origin e.g. `window.location.origin` — `http(s)://host[:port]`
 * @returns {string}
 */
export function encodePayloadToUrl(payload, origin) {
	const compact = JSON.stringify(payload);
	return String(origin || "") + SHARE_ROUTE + "#" + encodeURIComponent(compact);
}

/**
 * Decode a share input. Accepts either:
 *  - a URL produced by {@link encodePayloadToUrl} — the hash is extracted and JSON-decoded
 *  - a JSON string produced by {@link encodePayloadToJson}
 *  - a `#`-prefixed hash fragment (URL-encoded JSON)
 *
 * Returns the decoded payload (validated to carry `endpoint.url` and `cubeId`). Throws on
 * malformed input — the caller is expected to surface the error to the user.
 *
 * @param {string} input
 * @returns {{ endpoint: { url: string, name: string }, cubeId: string, query: Object }}
 */
export function decodeShareInput(input) {
	if (!input || typeof input !== "string") {
		throw new Error("Share input is empty");
	}
	const trimmed = input.trim();
	let jsonText;
	if (trimmed.startsWith("{")) {
		jsonText = trimmed;
	} else if (trimmed.startsWith("#")) {
		jsonText = safeDecodeUriComponent(trimmed.substring(1));
	} else if (/^https?:\/\//i.test(trimmed) || trimmed.startsWith("/")) {
		// Pull the hash from a URL-like string. Manual parse rather than `new URL(...)` so a
		// relative path (e.g. "/html/share#…") works without an explicit base.
		const hashAt = trimmed.indexOf("#");
		if (hashAt < 0) {
			throw new Error("URL has no #-fragment");
		}
		jsonText = safeDecodeUriComponent(trimmed.substring(hashAt + 1));
	} else {
		throw new Error("Unrecognised share input — paste the URL, the #-hash, or the JSON blob");
	}
	const parsed = JSON.parse(jsonText);
	validatePayload(parsed);
	return parsed;
}

/**
 * Best-effort URI-decode that falls back to the raw text on a malformed sequence. Mirrors the
 * approach in {@code queryHelper.readUrlHash}.
 */
function safeDecodeUriComponent(text) {
	try {
		return decodeURIComponent(text);
	} catch {
		return text;
	}
}

function validatePayload(payload) {
	if (!payload || typeof payload !== "object") {
		throw new Error("Share payload is not a JSON object");
	}
	if (!payload.endpoint || !payload.endpoint.url) {
		throw new Error("Share payload is missing endpoint.url");
	}
	if (!payload.cubeId) {
		throw new Error("Share payload is missing cubeId");
	}
}

/**
 * Locate an already-registered endpoint matching the payload's `url`. Searches both the
 * server-discovered endpoints and the user's local endpoints. Returns the endpoint id when
 * found, `null` otherwise. Match is by exact URL string — the `:self` sentinel and any
 * regular `http://host:port[/prefix]` URL are compared verbatim.
 *
 * @param {{ endpoint: { url: string } }} payload
 * @param {Record<string, any>} endpoints server-discovered
 * @param {Record<string, any>} localEndpoints user-registered
 * @returns {string | null}
 */
export function findKnownEndpointId(payload, endpoints, localEndpoints) {
	const target = payload && payload.endpoint;
	if (!target || !target.url) return null;
	const targetUrl = String(target.url);
	const candidates = [...Object.values(localEndpoints || {}), ...Object.values(endpoints || {})];
	for (const candidate of candidates) {
		if (!candidate) continue;
		if (String(candidate.url || "") === targetUrl) {
			return candidate.id;
		}
	}
	return null;
}

/**
 * Parse the share payload's `endpoint.url` into the `{host, port, prefix}` shape accepted by
 * {@code preferencesStore.addLocalEndpoint}. Returns `null` when the url is unparseable — that
 * happens for the sentinel `http://localhost:self` form, which is server-only and should never
 * need local registration (the recipient already has its own self-entry).
 *
 * @param {string} url
 * @returns {{ host: string, port: number, prefix: string } | null}
 */
export function parseEndpointUrl(url) {
	const match = /^https?:\/\/([^:\/]+)(?::(\d+))?(\/.*)?$/.exec(String(url || ""));
	if (!match) return null;
	return {
		host: match[1],
		port: match[2] ? Number(match[2]) : 8080,
		prefix: match[3] || "",
	};
}

/**
 * Build the destination route a registered endpoint+cube+query should land on. The query model
 * is wrapped under a `query` key so the destination page's hash-to-queryModel hydrator
 * (`queryHelper.hashToQueryModel`) picks it up on mount.
 *
 * <p>The hash is returned in RAW (unencoded) form, e.g. `#{"query":{...}}`. Callers MUST hand
 * it to vue-router's `push({hash})` / `replace({hash})` (which encodes it exactly once when
 * writing the URL) or to `encodeURIComponent` before a manual `history.pushState`. Passing an
 * already-encoded hash through vue-router produces a double-encoded URL — the wizard then
 * fails to JSON-parse the hash and the view loads with no query.
 *
 * @param {string} endpointId
 * @param {string} cubeId
 * @param {Object} query the `query` field of the share payload (already in parsed-json form)
 * @returns {{ path: string, hash: string }} a route fragment + raw `#`-prefixed hash
 */
export function buildQueryDestination(endpointId, cubeId, query) {
	const path = "/html/endpoints/" + encodeURIComponent(endpointId) + "/cubes/" + encodeURIComponent(cubeId) + "/query";
	const hash = "#" + JSON.stringify({ query: query || {} });
	return { path, hash };
}

export { SHARE_ROUTE };
