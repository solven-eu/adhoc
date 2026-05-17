// @ts-check

/**
 * Default shape of the "executor live state" bus shared between AdhocQueryExecutor (writer) and
 * AdhocGridControls (reader). AdhocQuery owns the reactive instance and provides it under the
 * "executorBus" injection key; AdhocGridControls uses this same shape as the inject default so
 * it still renders correctly when mounted outside an AdhocQuery scope (typically: tests).
 *
 * IMPORTANT: every flag MUST be a plain boolean (not a ref-like `{ value: false }` object). A
 * past version used per-flag inject tokens defaulting to `{ value: false }`, which is *truthy*
 * when evaluated as `v-if`. The grid-controls Refresh button consequently rendered a permanent
 * "Refreshing…" spinner the moment the wizard was hidden, even though no query was in flight.
 * `defaultExecutorBus()` is the single source of truth pinning that the defaults stay falsy.
 *
 * @returns {{
 *   isQueryInFlight: boolean,
 *   isSameAsLastQuery: boolean,
 *   autoQuery: boolean,
 *   submitQuery: () => void,
 * }} a fresh bag of default executor-bus values
 */
export function defaultExecutorBus() {
	return {
		isQueryInFlight: false,
		isSameAsLastQuery: false,
		autoQuery: false,
		submitQuery: () => {},
	};
}
