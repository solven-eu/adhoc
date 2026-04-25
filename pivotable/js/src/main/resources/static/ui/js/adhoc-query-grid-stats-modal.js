import { ref, computed } from "vue";

// Per-measure descriptive-statistics modal. Triggered from the grid measure column
// header (a `bi-bar-chart` button) — see `adhoc-query-grid-helper.js`. Receives the
// pre-computed stats object via a singleton model (`statsModel`) so a click in the
// header just sets `statsModel.measureName` / `statsModel.stats` and shows the modal,
// without round-tripping through the data.
//
// Why a modal and not a popover: stats include sum + variance + non-numeric counts,
// which is more text than a popover can carry without being claustrophobic; the
// modal gives space for a clean two-column layout and leaves room to add things like
// a histogram later.
export default {
	props: {
		statsModel: {
			type: Object,
			required: true,
		},
		formatOptions: {
			type: Object,
			required: false,
			default: () => ({}),
		},
	},
	setup(props) {
		// Reuse the grid's locale-aware Intl.NumberFormat so the modal numbers line up
		// visually with the cells in the column. Built lazily so toggling formatOptions
		// (currency, fraction digits) propagates without remounting the modal.
		const numberFormat = computed(() => {
			const opts = {};
			if (typeof props.formatOptions.minimumFractionDigits === "number") {
				opts.minimumFractionDigits = props.formatOptions.minimumFractionDigits;
			}
			if (typeof props.formatOptions.maximumFractionDigits === "number") {
				opts.maximumFractionDigits = props.formatOptions.maximumFractionDigits;
			}
			if (props.formatOptions.measureCcy) {
				opts.style = "currency";
				opts.currency = props.formatOptions.measureCcy;
			}
			try {
				return new Intl.NumberFormat(props.formatOptions.locale, opts);
			} catch (e) {
				return new Intl.NumberFormat(props.formatOptions.locale, {});
			}
		});

		const fmt = function (n) {
			if (n === null || n === undefined || Number.isNaN(n)) return "—";
			return numberFormat.value.format(n);
		};

		// Standard deviation derived from variance — easier to interpret than variance
		// for users not coming from a stats background.
		const stddev = computed(() => {
			const s = props.statsModel.stats;
			if (!s || typeof s.variance !== "number") return null;
			return Math.sqrt(s.variance);
		});

		return { fmt, stddev };
	},
	template: /* HTML */ `
		<div class="modal fade" id="measureStatsModal" tabindex="-1" aria-labelledby="measureStatsModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="measureStatsModalLabel">
							<i class="bi bi-bar-chart me-2"></i>Statistics — <span class="font-monospace">{{statsModel.measureName}}</span>
						</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<div v-if="!statsModel.stats" class="text-muted">No statistics available — submit a query first.</div>
						<table v-else class="table table-sm">
							<tbody>
								<tr>
									<th class="text-muted" style="width: 40%">Count (numeric)</th>
									<td class="text-end font-monospace">{{statsModel.stats.count}}</td>
								</tr>
								<tr>
									<th class="text-muted">Min</th>
									<td class="text-end font-monospace">{{fmt(statsModel.stats.min)}}</td>
								</tr>
								<tr>
									<th class="text-muted">Max</th>
									<td class="text-end font-monospace">{{fmt(statsModel.stats.max)}}</td>
								</tr>
								<tr>
									<th class="text-muted">Sum</th>
									<td class="text-end font-monospace">{{fmt(statsModel.stats.sum)}}</td>
								</tr>
								<tr>
									<th class="text-muted">Mean</th>
									<td class="text-end font-monospace">{{fmt(statsModel.stats.mean)}}</td>
								</tr>
								<tr>
									<th class="text-muted">Variance</th>
									<td class="text-end font-monospace">{{fmt(statsModel.stats.variance)}}</td>
								</tr>
								<tr>
									<th class="text-muted">Std. dev.</th>
									<td class="text-end font-monospace">{{fmt(stddev)}}</td>
								</tr>
								<tr v-if="statsModel.stats.nullCount > 0">
									<th class="text-warning">Nulls</th>
									<td class="text-end font-monospace">{{statsModel.stats.nullCount}}</td>
								</tr>
								<tr v-if="statsModel.stats.nonNumericCount > 0">
									<th class="text-warning">Non-numeric (strings, NaN)</th>
									<td class="text-end font-monospace">{{statsModel.stats.nonNumericCount}}</td>
								</tr>
							</tbody>
						</table>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
