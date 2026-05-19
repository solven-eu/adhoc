// @ts-check
import { describe, it, expect, beforeEach, vi } from "vitest";

// We need a `document` and a `window`. The default vitest env is `node` (per the project setup),
// so wire a stub before importing the module under test.
const buildStubDom = ({ hidden = true, focused = false } = {}) => {
	const nodes = new Map();
	/** @type {Record<string, any>} */
	const docStub = {
		hidden,
		visibilityState: hidden ? "hidden" : "visible",
		_focused: focused,
		_listeners: /** @type {Record<string, Function[]>} */ ({}),
		addEventListener(event, fn) {
			(docStub._listeners[event] = docStub._listeners[event] || []).push(fn);
		},
		dispatch(event) {
			(docStub._listeners[event] || []).forEach((fn) => fn());
		},
		hasFocus() {
			return docStub._focused;
		},
		createElement(tag) {
			const el = {
				tagName: tag,
				id: "",
				rel: "",
				type: "",
				href: "",
				dataset: /** @type {Record<string, string>} */ ({}),
				parentNode: null,
				getContext: () => ({
					beginPath: vi.fn(),
					arc: vi.fn(),
					fill: vi.fn(),
					stroke: vi.fn(),
					drawImage: vi.fn(),
					fillStyle: "",
					strokeStyle: "",
					lineWidth: 0,
				}),
				toDataURL: () => "data:image/png;base64,STUB",
				width: 0,
				height: 0,
			};
			return el;
		},
		getElementById(id) {
			return nodes.get(id) || null;
		},
		head: {
			appendChild(el) {
				if (el.id) nodes.set(el.id, el);
				el.parentNode = { removeChild: (child) => nodes.delete(child.id) };
			},
		},
	};
	// @ts-ignore — patching the global so the module under test sees the stub
	globalThis.document = docStub;
	const windowStub = /** @type {any} */ ({
		_listeners: /** @type {Record<string, Function[]>} */ ({}),
		addEventListener(event, fn) {
			(windowStub._listeners[event] = windowStub._listeners[event] || []).push(fn);
		},
		dispatch(event) {
			(windowStub._listeners[event] || []).forEach((fn) => fn());
		},
		AudioContext: function () {
			return {
				currentTime: 0,
				createOscillator: () => ({ connect: () => ({ connect: () => {} }), start: vi.fn(), stop: vi.fn(), frequency: { value: 0 }, type: "" }),
				createGain: () => ({ connect: () => ({ connect: () => ({}) }), gain: { setValueAtTime: vi.fn(), linearRampToValueAtTime: vi.fn() } }),
				destination: {},
			};
		},
	});
	// @ts-ignore
	globalThis.window = windowStub;
	// @ts-ignore — Image used inside buildBadgedDataUrl. We fire `onerror` so the helper draws
	// the badge alone — the data-URL contents don't matter to the assertions.
	globalThis.Image = function () {
		const self = /** @type {any} */ (this);
		self.onload = null;
		self.onerror = null;
		Object.defineProperty(self, "src", {
			set() {
				setTimeout(() => self.onerror && self.onerror(), 0);
			},
		});
	};
	return { docStub, windowStub };
};

describe("tab-notification", () => {
	beforeEach(() => {
		vi.resetModules();
	});

	it("notifyIfBackground is a no-op when the tab is visible AND the window has focus", async () => {
		const { docStub } = buildStubDom({ hidden: false, focused: true });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		mod.notifyIfBackground();
		await new Promise((r) => setTimeout(r, 5));
		expect(docStub.getElementById("adhoc-favicon-badged")).toBeNull();
	});

	it("notifyIfBackground fires when the tab is hidden", async () => {
		const { docStub } = buildStubDom({ hidden: true, focused: true });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		mod.notifyIfBackground();
		await new Promise((r) => setTimeout(r, 5));
		const link = docStub.getElementById("adhoc-favicon-badged");
		expect(link).toBeTruthy();
		expect(link.dataset.adhocBadge).toBe("on");
	});

	it("notifyIfBackground fires when the window is not focused (other OS window in front)", async () => {
		const { docStub } = buildStubDom({ hidden: false, focused: false });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		mod.notifyIfBackground();
		await new Promise((r) => setTimeout(r, 5));
		expect(docStub.getElementById("adhoc-favicon-badged")).toBeTruthy();
	});

	it("setBadge is idempotent — repeated calls reuse the same <link>", async () => {
		const { docStub } = buildStubDom();
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		await mod.setBadge();
		await mod.setBadge();
		await mod.setBadge();
		await new Promise((r) => setTimeout(r, 5));
		expect(docStub.getElementById("adhoc-favicon-badged")).toBeTruthy();
	});

	it("clearBadge restores the favicon by swapping the <link> href back to /favicon.ico (the element stays)", async () => {
		const { docStub } = buildStubDom();
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		await mod.setBadge();
		await new Promise((r) => setTimeout(r, 5));
		mod.clearBadge();
		const link = docStub.getElementById("adhoc-favicon-badged");
		expect(link).toBeTruthy();
		expect(link.href.startsWith("/favicon.ico?adhoc=")).toBe(true);
		expect(link.dataset.adhocBadge).toBe("off");
	});

	it("becoming visible (tab switch) auto-clears the badge", async () => {
		const { docStub } = buildStubDom({ hidden: true });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		await mod.setBadge();
		await new Promise((r) => setTimeout(r, 5));
		expect(docStub.getElementById("adhoc-favicon-badged").dataset.adhocBadge).toBe("on");
		docStub.hidden = false;
		docStub.visibilityState = "visible";
		docStub.dispatch("visibilitychange");
		expect(docStub.getElementById("adhoc-favicon-badged").dataset.adhocBadge).toBe("off");
	});

	it("setBadge skips the badge when the user has refocused during the async favicon build", async () => {
		// User starts in the background — notifyIfBackground fires setBadge, which kicks off the
		// async data-URL build. While that's still pending, the user comes back to the page.
		const { docStub } = buildStubDom({ hidden: true, focused: false });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		const promise = mod.setBadge();
		// Simulate the user refocusing before the Image onerror callback fires (the stub uses
		// setTimeout, so the foreground flip happens on the same tick as the pending callback).
		docStub.hidden = false;
		docStub.visibilityState = "visible";
		docStub._focused = true;
		await promise;
		await new Promise((r) => setTimeout(r, 5));
		// No `<link>` should have been injected — the post-async background re-check bails out.
		expect(docStub.getElementById("adhoc-favicon-badged")).toBeNull();
	});

	it("window regaining focus auto-clears the badge (other-OS-window case)", async () => {
		const { docStub, windowStub } = buildStubDom({ hidden: false, focused: false });
		const mod = await import("../src/main/resources/static/ui/js/adhoc-tab-notification.js");
		await mod.setBadge();
		await new Promise((r) => setTimeout(r, 5));
		expect(docStub.getElementById("adhoc-favicon-badged").dataset.adhocBadge).toBe("on");
		docStub._focused = true;
		windowStub.dispatch("focus");
		expect(docStub.getElementById("adhoc-favicon-badged").dataset.adhocBadge).toBe("off");
	});
});
