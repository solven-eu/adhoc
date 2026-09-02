// @ts-check
import { computed } from "vue";

import { involvedInQuery, mentionedInError } from "./adhoc-query-involved.js";

// Shown inside the "Query is broken" banner: what the failing query actually carries, with a one-click way to drop
// any of it.
//
// The wizard already allows unpicking a measure, but it means leaving the error, finding the row among possibly
// hundreds, and unchecking it — while the message naming the culprit scrolls out of view. Listing the query's own
// contents next to the message closes that loop.
//
// Names occurring in the error text are marked. That is a substring match, not a diagnosis, so it is presented as a
// suggestion ("mentioned in the error") rather than as the cause.
export default {
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
		errorMessage: {
			type: String,
			required: false,
			default: "",
		},
	},
	setup(props) {
		const involved = computed(() => involvedInQuery(props.queryModel));

		const isMentioned = function (name) {
			return mentionedInError(props.errorMessage, name);
		};

		/** Ordered so anything the error names comes first — that is what the user is looking for. */
		const orderByMention = function (names) {
			return [...names].sort((left, right) => Number(isMentioned(right)) - Number(isMentioned(left)));
		};

		const measures = computed(() => orderByMention(involved.value.measures));
		const columns = computed(() => orderByMention(involved.value.columns));

		const removeMeasure = function (measureName) {
			props.queryModel.selectedMeasures[measureName] = false;
		};

		const removeColumn = function (columnName) {
			props.queryModel.selectedColumns[columnName] = false;
			// Mirrors the grid's own remove-column path, which notifies the model so dependent state is refreshed.
			if (typeof props.queryModel.onColumnToggled === "function") {
				props.queryModel.onColumnToggled(columnName);
			}
		};

		return { measures, columns, isMentioned, removeMeasure, removeColumn };
	},
	template: /* HTML */ `
		<div class="mt-2">
			<div class="small fw-semibold">In this query — click to remove</div>

			<div v-if="measures.length > 0" class="mt-1">
				<span class="small text-muted me-1">Measures:</span>
				<button
					v-for="measureName in measures"
					type="button"
					class="btn btn-sm py-0 px-1 me-1 mb-1 border"
					:class="isMentioned(measureName) ? 'btn-warning fw-semibold' : 'btn-light'"
					:title="isMentioned(measureName) ? 'Mentioned in the error message — remove this measure from the query' : 'Remove this measure from the query'"
					@click="removeMeasure(measureName)"
				>
					<span class="font-monospace">{{measureName}}</span>
					<i class="bi bi-x ms-1"></i>
				</button>
			</div>

			<div v-if="columns.length > 0" class="mt-1">
				<span class="small text-muted me-1">Columns:</span>
				<button
					v-for="columnName in columns"
					type="button"
					class="btn btn-sm py-0 px-1 me-1 mb-1 border"
					:class="isMentioned(columnName) ? 'btn-warning fw-semibold' : 'btn-light'"
					:title="isMentioned(columnName) ? 'Mentioned in the error message — remove this column from the groupBy' : 'Remove this column from the groupBy'"
					@click="removeColumn(columnName)"
				>
					<span class="font-monospace">{{columnName}}</span>
					<i class="bi bi-x ms-1"></i>
				</button>
			</div>

			<div v-if="measures.length === 0 && columns.length === 0" class="small text-muted mt-1">This query carries no measure and no column.</div>
		</div>
	`,
};
