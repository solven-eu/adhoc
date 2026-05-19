// @ts-check

// Lightweight "result is back" indicator for tabs/windows the user has navigated away from.
//
// Two channels:
//  - **Favicon badge**: a red dot is overlaid on top of the existing `/favicon.ico` and
//    injected as a data-URL via a `<link rel="icon">` element. Clearing the badge replaces
//    that same `<link>` with a cache-busted reference back to `/favicon.ico` — simply
//    removing the element does NOT trigger most browsers to re-fetch the original icon,
//    they keep showing whatever icon was last set programmatically.
//  - **Audio ping**: a 150ms two-tone Web Audio chime. No asset to ship; the OscillatorNode
//    synthesises it on the fly.
//
// `notifyIfBackground()` is the only entry point intended for callers. It triggers when
// EITHER the tab is hidden (`document.hidden`) OR the window does not have OS-level focus
// (`!document.hasFocus()`) — so it covers both "user switched to another tab" and "user is
// using a different app/window entirely". Foreground call sites are silent.
//
// Auto-clear listens to BOTH `visibilitychange` (tab switch) AND `window.focus` (the
// Pivotable window regaining OS focus), so the badge disappears the moment the user looks
// back at the page in any way.

const BADGED_LINK_ID = "adhoc-favicon-badged";
const FAVICON_URL = "/favicon.ico";
const FAVICON_SIZE = 32;
const BADGE_RADIUS = 7;
const BADGE_FILL = "#dc3545"; // Bootstrap danger red — same swatch the BackendStatusBanner uses.

/** @type {Promise<string> | null} Cached badged-favicon data-URL, lazily built on first use. */
let badgedDataUrlPromise = null;

/** @type {AudioContext | null} */
let audioCtx = null;

let visibilityHookInstalled = false;

/**
 * Draw the existing favicon onto an off-screen canvas with a red dot in the top-right corner.
 * Returns a data-URL ready to feed to a `<link rel="icon">`. The result is cached after the
 * first build — the favicon does not change at runtime.
 *
 * <p>Eagerly warmed at module load (see the bottom of this file) so the first call to
 * `setBadge()` doesn't have to wait for the favicon image to load — the data-URL is already
 * resolved by then, and the badge appears synchronously on the first query completion.
 *
 * <p>If the favicon load fails (offline, blocked, missing) we fall back to drawing the badge
 * alone on a transparent canvas. The dot is the actually-meaningful part of the signal.
 *
 * @returns {Promise<string>}
 */
function buildBadgedDataUrl() {
	if (badgedDataUrlPromise) return badgedDataUrlPromise;
	badgedDataUrlPromise = new Promise((resolve) => {
		const canvas = document.createElement("canvas");
		canvas.width = FAVICON_SIZE;
		canvas.height = FAVICON_SIZE;
		const ctx = canvas.getContext("2d");
		if (!ctx) {
			resolve("");
			return;
		}
		const drawBadge = () => {
			// Top-right red dot with a thin contrasting outline so it pops against light AND
			// dark favicons.
			const cx = FAVICON_SIZE - BADGE_RADIUS - 1;
			const cy = BADGE_RADIUS + 1;
			ctx.beginPath();
			ctx.arc(cx, cy, BADGE_RADIUS, 0, Math.PI * 2);
			ctx.fillStyle = BADGE_FILL;
			ctx.fill();
			ctx.lineWidth = 1.5;
			ctx.strokeStyle = "#ffffff";
			ctx.stroke();
			resolve(canvas.toDataURL("image/png"));
		};
		const img = new Image();
		img.onload = () => {
			ctx.drawImage(img, 0, 0, FAVICON_SIZE, FAVICON_SIZE);
			drawBadge();
		};
		img.onerror = () => {
			// No base favicon — draw the dot alone on a transparent canvas.
			drawBadge();
		};
		img.src = FAVICON_URL;
	});
	return badgedDataUrlPromise;
}

/** Install the auto-clear listeners exactly once. */
function ensureForegroundHooks() {
	if (visibilityHookInstalled) return;
	visibilityHookInstalled = true;
	// Tab-level: switching back to this tab from another tab in the same window.
	document.addEventListener("visibilitychange", () => {
		if (document.visibilityState === "visible") {
			clearBadge();
		}
	});
	// Window-level: the Pivotable window regaining OS focus from another app / window.
	// `document.hidden` may stay false in that scenario, so `visibilitychange` alone is not
	// enough — we also listen for the window's `focus` event.
	if (typeof window !== "undefined" && typeof window.addEventListener === "function") {
		window.addEventListener("focus", () => {
			clearBadge();
		});
	}
}

/**
 * Replace the favicon with the badged version. No-op when the badge is already shown so
 * repeated calls (e.g. a flurry of completing queries) don't thrash the DOM. The `<link>`
 * element is reused across set/clear cycles so the browser sees a single rel="icon" node
 * whose `href` swaps between the data-URL and the original favicon.
 *
 * <p>Race-window guard: the badged data-URL is built asynchronously (the favicon image must
 * load first). If the user returns to the page during that window, the auto-clear listeners
 * have nothing to clear yet (the `<link>` hasn't been created). We therefore re-check the
 * background state after the async build and skip applying the badge if we're already back
 * in the foreground — otherwise a query that completes just as the user refocuses would
 * leave a stale badge on the tab.
 */
export async function setBadge() {
	ensureForegroundHooks();
	const existing = /** @type {HTMLLinkElement | null} */ (document.getElementById(BADGED_LINK_ID));
	if (existing && existing.dataset && existing.dataset.adhocBadge === "on") return;
	const dataUrl = await buildBadgedDataUrl();
	if (!dataUrl) return;
	if (!isInBackground()) return;
	let link = /** @type {HTMLLinkElement | null} */ (document.getElementById(BADGED_LINK_ID));
	if (!link) {
		link = /** @type {HTMLLinkElement} */ (document.createElement("link"));
		link.id = BADGED_LINK_ID;
		link.rel = "icon";
		document.head.appendChild(link);
	}
	link.type = "image/png";
	link.href = dataUrl;
	if (link.dataset) link.dataset.adhocBadge = "on";
}

/**
 * Restore the original favicon. Does NOT remove the `<link>` element — many browsers ignore
 * a stale-cached `/favicon.ico` once a programmatic icon has been set, so we explicitly
 * swap the `<link>` back to a cache-busted reference at `/favicon.ico?adhoc=<timestamp>`.
 * The query string forces a fresh fetch on the first restore; subsequent restores reuse the
 * same bust value to avoid hammering the network.
 */
export function clearBadge() {
	const link = /** @type {HTMLLinkElement | null} */ (document.getElementById(BADGED_LINK_ID));
	if (!link) return;
	if (link.dataset && link.dataset.adhocBadge !== "on") return;
	link.type = "image/x-icon";
	link.href = FAVICON_URL + "?adhoc=" + Date.now();
	if (link.dataset) link.dataset.adhocBadge = "off";
}

/**
 * Play a short two-tone chime via the Web Audio API. Returns a promise that resolves when the
 * sound finishes (or fails silently if the AudioContext can't start — which happens when the
 * page hasn't yet received a user gesture). No asset to ship.
 *
 * @returns {Promise<void>}
 */
export function playPing() {
	return new Promise((resolve) => {
		try {
			const AC = window.AudioContext || /** @type {any} */ (window).webkitAudioContext;
			if (!AC) return resolve();
			if (!audioCtx) audioCtx = new AC();
			const now = audioCtx.currentTime;
			const tones = [
				{ freq: 880, start: 0, dur: 0.09 },
				{ freq: 1318, start: 0.08, dur: 0.12 },
			];
			for (const tone of tones) {
				const osc = audioCtx.createOscillator();
				const gain = audioCtx.createGain();
				osc.type = "sine";
				osc.frequency.value = tone.freq;
				// Quick fade-in/out so the tone doesn't click.
				gain.gain.setValueAtTime(0, now + tone.start);
				gain.gain.linearRampToValueAtTime(0.18, now + tone.start + 0.01);
				gain.gain.linearRampToValueAtTime(0, now + tone.start + tone.dur);
				osc.connect(gain).connect(audioCtx.destination);
				osc.start(now + tone.start);
				osc.stop(now + tone.start + tone.dur);
			}
			setTimeout(resolve, 250);
		} catch (e) {
			// Audio blocked or unsupported — silently skip; the favicon badge still does its job.
			resolve();
		}
	});
}

/**
 * True when the Pivotable view is in the background — either the tab is hidden behind
 * another tab in the same window, OR the window itself does not have OS-level focus (the
 * user is using a different app or browser window). Both branches need to be covered:
 * `document.hidden` does NOT flip when the user just switches to another OS window.
 */
function isInBackground() {
	if (typeof document === "undefined") return false;
	if (document.hidden) return true;
	if (typeof document.hasFocus === "function" && !document.hasFocus()) return true;
	return false;
}

/**
 * Convenience: badge + ping iff the Pivotable view is in the background (hidden tab OR
 * unfocused window). Safe to call from every query completion path — both success and
 * failure — without spamming foreground users.
 */
export function notifyIfBackground() {
	if (!isInBackground()) return;
	setBadge();
	playPing();
}

// Back-compat alias for callers wired before the window-focus path was added.
export const notifyIfHidden = notifyIfBackground;

// Pre-warm the badged favicon data-URL at module load. The build is asynchronous (the source
// favicon must load through an `Image` element), and if we leave it to the first `setBadge`
// call the badge appears with a visible lag — a query that completes a few hundred ms after
// page load would finish faster than the favicon does, and the tab would only flip its icon
// after the user is already looking away. Kicking the build off now lets the data-URL be ready
// by the time any realistic query returns, so `await buildBadgedDataUrl()` resolves
// synchronously in `setBadge` from then on.
if (typeof document !== "undefined" && typeof Image !== "undefined") {
	// Fire-and-forget — errors are absorbed by the helper itself (badge-alone fallback).
	buildBadgedDataUrl();
}
