// @ts-check
import { describe, it, expect } from "vitest";

import {
	buildSharePayload,
	encodePayloadToJson,
	encodePayloadToUrl,
	decodeShareInput,
	findKnownEndpointId,
	buildQueryDestination,
	parseEndpointUrl,
	SHARE_ROUTE,
} from "../src/main/resources/static/ui/js/adhoc-share-helper.js";

// Minimum queryModel shim that satisfies queryHelper.queryModelToParsedJson without dragging
// the full wizard reactive object in.
const fakeQueryModel = () => ({
	columns: () => ({ 0: "color" }),
	withStarColumns: {},
	measures: () => ({ count: true }),
	filter: {},
	customMarkers: {},
	options: () => ["EXPLAIN"],
});

describe("buildSharePayload", () => {
	it("uses endpoint.url verbatim when present (server-discovered shape)", () => {
		const payload = buildSharePayload({ url: "http://h:1234/api", name: "X" }, "C1", fakeQueryModel());
		expect(payload.endpoint).toEqual({ url: "http://h:1234/api", name: "X" });
		expect(payload.cubeId).toBe("C1");
		expect(payload.query.measures).toEqual({ count: true });
		expect(payload.query.options).toEqual(["EXPLAIN"]);
	});

	it("synthesises url from host/port/prefix (local-endpoint shape)", () => {
		const payload = buildSharePayload({ host: "h", port: 1234, prefix: "/api", name: "X" }, "C1", fakeQueryModel());
		expect(payload.endpoint.url).toBe("http://h:1234/api");
		expect(payload.endpoint.name).toBe("X");
	});

	it("defaults missing port/prefix to 8080 and empty string", () => {
		const payload = buildSharePayload({ host: "h" }, "C1", fakeQueryModel());
		expect(payload.endpoint.url).toBe("http://h:8080");
	});

	it("requires either url or host", () => {
		// @ts-ignore intentionally invalid
		expect(() => buildSharePayload({ name: "X" }, "C1", fakeQueryModel())).toThrow();
	});

	it("preserves the :self sentinel URL verbatim", () => {
		const payload = buildSharePayload({ url: "http://localhost:self", name: "self" }, "C1", fakeQueryModel());
		expect(payload.endpoint.url).toBe("http://localhost:self");
	});
});

describe("encode / decode round-trip", () => {
	const payload = {
		endpoint: { url: "http://h:8080", name: "" },
		cubeId: "C1",
		query: { columns: { 0: "x" } },
	};

	it("URL round-trips via decodeShareInput", () => {
		const url = encodePayloadToUrl(payload, "http://localhost:5173");
		expect(url).toContain(SHARE_ROUTE + "#");
		expect(decodeShareInput(url)).toEqual(payload);
	});

	it("JSON round-trips via decodeShareInput", () => {
		const json = encodePayloadToJson(payload);
		expect(decodeShareInput(json)).toEqual(payload);
	});

	it("bare #hash round-trips", () => {
		const url = encodePayloadToUrl(payload, "http://x");
		const hash = url.substring(url.indexOf("#"));
		expect(decodeShareInput(hash)).toEqual(payload);
	});
});

describe("decodeShareInput validation", () => {
	it("rejects empty input", () => {
		expect(() => decodeShareInput("")).toThrow();
	});

	it("rejects a URL without #fragment", () => {
		expect(() => decodeShareInput("http://localhost/html/share")).toThrow(/#-fragment/);
	});

	it("rejects unrecognised text", () => {
		expect(() => decodeShareInput("just some text")).toThrow();
	});

	it("rejects a payload missing endpoint.url", () => {
		const bad = JSON.stringify({ endpoint: {}, cubeId: "C1", query: {} });
		expect(() => decodeShareInput(bad)).toThrow(/endpoint.url/);
	});

	it("rejects a payload missing cubeId", () => {
		const bad = JSON.stringify({ endpoint: { url: "http://h" }, query: {} });
		expect(() => decodeShareInput(bad)).toThrow(/cubeId/);
	});
});

describe("findKnownEndpointId", () => {
	const payload = { endpoint: { url: "http://h:8080", name: "" } };

	it("matches a local endpoint by url", () => {
		const result = findKnownEndpointId(payload, {}, { L1: { id: "L1", url: "http://h:8080" } });
		expect(result).toBe("L1");
	});

	it("matches a server-discovered endpoint by url", () => {
		const result = findKnownEndpointId(payload, { E1: { id: "E1", url: "http://h:8080" } }, {});
		expect(result).toBe("E1");
	});

	it("matches the :self sentinel verbatim", () => {
		const selfPayload = { endpoint: { url: "http://localhost:self", name: "" } };
		const result = findKnownEndpointId(selfPayload, { S: { id: "S", url: "http://localhost:self" } }, {});
		expect(result).toBe("S");
	});

	it("returns null when no candidate matches", () => {
		const result = findKnownEndpointId(payload, {}, { L1: { id: "L1", url: "http://other:8080" } });
		expect(result).toBeNull();
	});

	it("prefers a local match over a server-discovered duplicate", () => {
		const result = findKnownEndpointId(payload, { E1: { id: "E1", url: "http://h:8080" } }, { L1: { id: "L1", url: "http://h:8080" } });
		expect(result).toBe("L1");
	});
});

describe("parseEndpointUrl", () => {
	it("splits a regular URL into host/port/prefix", () => {
		expect(parseEndpointUrl("http://h:8080/api")).toEqual({ host: "h", port: 8080, prefix: "/api" });
	});

	it("defaults port to 8080 when absent", () => {
		expect(parseEndpointUrl("http://h")).toEqual({ host: "h", port: 8080, prefix: "" });
	});

	it("returns null for the :self sentinel (non-numeric port)", () => {
		expect(parseEndpointUrl("http://localhost:self")).toBeNull();
	});

	it("returns null for non-URL input", () => {
		expect(parseEndpointUrl("not a url")).toBeNull();
	});
});

describe("buildQueryDestination", () => {
	it("returns the hash in raw (unencoded) form for vue-router", () => {
		const dest = buildQueryDestination("E1", "C1", { measures: { count: true } });
		expect(dest.path).toBe("/html/endpoints/E1/cubes/C1/query");
		// Raw hash — directly JSON-parseable after stripping the leading `#`.
		expect(dest.hash.startsWith("#")).toBe(true);
		const decoded = JSON.parse(dest.hash.substring(1));
		expect(decoded.query.measures).toEqual({ count: true });
		// And critically, NOT pre-encoded — no `%7B` etc.
		expect(dest.hash).not.toContain("%");
	});

	it("encodes special characters in endpointId / cubeId in the path", () => {
		const dest = buildQueryDestination("E with space", "C/slash", {});
		expect(dest.path).toBe("/html/endpoints/E%20with%20space/cubes/C%2Fslash/query");
	});
});
