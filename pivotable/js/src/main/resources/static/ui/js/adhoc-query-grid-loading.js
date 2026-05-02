// Pure helpers derived from `tabularView.loading`, extracted from `adhoc-query-grid.js` so they
// can be unit-tested without a DOM. Each function reads the loading object produced by
// `adhoc-query-executor.js` and the grid hooks, and returns a display-layer primitive (bool /
// number / string) for the grid's loading overlay.
//
// Shape of `tabularView.loading` (all boolean flags, potentially missing):
//   { sending, executing, fetching, sleeping, downloading, preparingGrid, rendering, sorting,
//     rowSpanning, … }
// Plus non-boolean fields such as `latestFetched` (Date) — we explicitly skip those in
// `isLoading` so they don't pin the progress bar at "loading" forever.

// True when at least one boolean flag inside `tabularView.loading` is truthy. Non-boolean
// fields (e.g. `latestFetched`) are ignored on purpose so they don't look "stuck" loading.
export const isLoading = function (tabularView) {
	const loading = tabularView && tabularView.loading;
	if (!loading) return false;
	return Object.values(loading).some((flag) => typeof flag === "boolean" && !!flag);
};

// Progress-bar position as a percentage (0-100). The mapping mirrors the typical lifecycle of
// a query round-trip — sending → executing → downloading → preparingGrid → rendering — so the
// bar advances monotonically. Returns 100 when nothing is loading.
export const loadingPercent = function (tabularView) {
	if (!isLoading(tabularView)) return 100;
	const loading = tabularView.loading;

	if (loading.sending) return 10;
	if (loading.executing) return 20;
	if (loading.downloading) return 75;
	if (loading.preparingGrid) return 85;
	if (loading.rendering) return 90;

	// Catch-all for transitional / unknown states.
	return 95;
};

// Human-readable label for the current loading stage. Returns "Loaded" when idle, and a
// stage-specific sentence otherwise. The `executing` stage has two sub-states (fetching vs.
// sleeping during a polling loop) reflected in the message.
export const loadingMessage = function (tabularView) {
	if (!isLoading(tabularView)) return "Loaded";
	const loading = tabularView.loading;

	if (loading.sending) return "Sending the query";
	if (loading.executing) {
		if (loading.fetching) return "Executing the query (fetching)";
		if (loading.sleeping) return "Executing the query (sleeping)";
		return "Executing the query (?)";
	}
	if (loading.downloading) return "Downloading the result";
	if (loading.preparingGrid) return "Preparing the grid";
	if (loading.rendering) return "Rendering the grid";

	return "Unclear but not done yet";
};

export default { isLoading, loadingPercent, loadingMessage };
