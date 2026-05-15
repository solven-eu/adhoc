// @ts-check
export default {
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		formatOptions: {
			type: Object,
			required: true,
		},
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
		<form>
			<div>
				Locale:
				<input class="form-control mr-sm-2" type="text" placeholder="Locale" aria-label="Locale" id="locale" v-model="formatOptions.locale" />
			</div>
			<div>
				Currency:
				<input class="form-control mr-sm-2" type="text" placeholder="Currency" aria-label="Currency" id="currency" v-model="formatOptions.measureCcy" />
			</div>
			<div>
				Minimum fraction digits:
				<input
					class="form-control mr-sm-2"
					type="text"
					placeholder="Min fraction digits"
					aria-label="Min fraction digits"
					id="minimumFractionDigits"
					v-model.number="formatOptions.minimumFractionDigits"
				/>
			</div>
			<div>
				Max fraction digits:
				<input
					class="form-control mr-sm-2"
					type="text"
					placeholder="Max fraction digits"
					aria-label="Max fraction digits"
					id="maximumFractionDigits"
					v-model.number="formatOptions.maximumFractionDigits"
				/>
			</div>
			<div>
				Max significant digits (e.g. if '2', '1234' is shown as '1200'):
				<input
					class="form-control mr-sm-2"
					type="text"
					placeholder="Max significant digits"
					aria-label="Max significant digits"
					id="maximumSignificantDigits"
					v-model.number="formatOptions.maximumSignificantDigits"
				/>
			</div>
			<div>
				roundingPriority
				<input
					class="form-control mr-sm-2"
					type="text"
					placeholder="roundingPriority"
					aria-label="roundingPriority"
					id="roundingPriority"
					v-model.number="formatOptions.roundingPriority"
				/>
			</div>
			<!--
				Heatmap toggles. Off by default. Independent flags:
				  - primaryHeatmap   : whole-column min/max background gradient.
				  - secondaryHeatmap : per-parent-group in-cell bar (only meaningful when the
				                       query has a hierarchical groupBy).
				Both applied in adhoc-query-grid-helper.js where the formatter reads
				formatOptions.primaryHeatmap / .secondaryHeatmap before invoking the helpers.
			-->
			<div class="form-check form-switch mt-2">
				<input class="form-check-input" type="checkbox" role="switch" id="formatPrimaryHeatmap" v-model="formatOptions.primaryHeatmap" />
				<label class="form-check-label" for="formatPrimaryHeatmap">Primary heatmap (colour cells by column min→max)</label>
			</div>
			<div class="form-check form-switch">
				<input class="form-check-input" type="checkbox" role="switch" id="formatSecondaryHeatmap" v-model="formatOptions.secondaryHeatmap" />
				<label class="form-check-label" for="formatSecondaryHeatmap">Secondary heatmap (bar inside each cell, by parent group)</label>
			</div>
		</form>
	`,
};
