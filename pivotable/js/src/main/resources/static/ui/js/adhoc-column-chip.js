import { computed, inject } from "vue";

import { Modal } from "bootstrap";

/**
 * Visible reference to a cube column, with a small action menu surfacing the operations a user is most likely to want
 * on that column from any place it appears in the UI:
 *
 *   - addAsGroupBy: toggle the column into queryModel.selectedColumns. Disabled when already selected.
 *   - addAsFilter:  open the column-filter modal so the user can compose a fresh filter on this column. Always enabled.
 *   - editFilter:   open the column-filter modal pre-populated with the current filter on this column. Disabled when
 *                   the column does not appear as a single AND-leaf in queryModel.filter (anything more complex —
 *                   nested OR / NOT, multiple occurrences — is left to the user to edit by hand).
 *
 * The component reads queryModel via Vue's inject() the same way the wizard's filter tree does, so it can be dropped
 * anywhere inside the wizard subtree without prop-drilling. Filter-widget operations (remove, replace value, negate)
 * deliberately do NOT live here — they belong on the filter chip itself, since they are operations on a filter, not on
 * the abstract column.
 */
export default {
	name: "AdhocColumnChip",
	props: {
		// The column name. Required.
		name: {
			type: String,
			required: true,
		},
		// Visually mark the chip as paused/disabled (line-through, muted). The chip's outer wrapper is
		// `d-inline-block` (atomic inline-level), and its inner `<a>` carries `text-decoration-none`, so
		// `text-decoration: line-through` applied to a parent does NOT propagate. Callers wanting the chip
		// to honour a paused-filter strikethrough must pass `:disabled="filter.disabled"` and let the chip
		// apply the decoration directly.
		disabled: {
			type: Boolean,
			default: false,
		},
	},
	setup(props) {
		const queryModel = inject("queryModel");

		const isGroupBy = computed(() => !!queryModel?.selectedColumns?.[props.name]);

		// editFilter is only enabled when the column appears as exactly one AND-leaf at the root of the filter tree.
		// Anything more complex (nested OR / NOT, multiple occurrences) would require disambiguation we are not yet
		// willing to do — the user can edit those by hand.
		const editableFilter = computed(() => {
			const filter = queryModel?.filter;
			if (!filter || !filter.type) return false;
			if (filter.type === "column" && filter.column === props.name) return true;
			if (filter.type === "and" && Array.isArray(filter.filters)) {
				const matches = filter.filters.filter((f) => f && f.type === "column" && f.column === props.name);
				return matches.length === 1;
			}
			return false;
		});

		const addAsGroupBy = function () {
			if (!queryModel) return;
			if (!queryModel.selectedColumns) queryModel.selectedColumns = {};
			queryModel.selectedColumns[props.name] = true;
		};

		const openColumnFilterModal = function () {
			const el = document.getElementById("columnFilterModal_" + props.name);
			if (!el) {
				console.warn("No columnFilterModal mounted for column", props.name);
				return;
			}
			new Modal(el, {}).show();
		};

		// addAsFilter and editFilter both delegate to the same modal — the modal already reads the current filter on
		// the column, so populating it with the existing filter (edit) vs starting fresh (add) is decided by the
		// modal, not by us. We keep two distinct menu entries so disabling editFilter is meaningful: it tells the
		// user "no single-leaf filter found, you have to add a fresh one".
		const addAsFilter = function () {
			openColumnFilterModal();
		};

		const editFilter = function () {
			openColumnFilterModal();
		};

		return { isGroupBy, editableFilter, addAsGroupBy, addAsFilter, editFilter };
	},
	template: /* HTML */ `
		<span class="dropdown d-inline-block" :class="disabled ? 'text-muted' : ''">
			<a
				href="#"
				class="fw-semibold"
				:class="disabled ? 'text-decoration-line-through text-muted' : 'text-decoration-none'"
				role="button"
				data-bs-toggle="dropdown"
				aria-expanded="false"
				:title="'Operate on column ' + name"
				@click.prevent
				>{{name}}</a
			>
			<ul class="dropdown-menu shadow-sm">
				<li>
					<button
						type="button"
						class="dropdown-item"
						:class="isGroupBy ? 'disabled' : ''"
						:disabled="isGroupBy"
						:title="isGroupBy ? 'Column is already in groupBy' : 'Add this column as a groupBy'"
						@click="addAsGroupBy"
					>
						<i class="bi bi-grid-3x2-gap me-1"></i> Add as groupBy
					</button>
				</li>
				<li>
					<button type="button" class="dropdown-item" title="Add a filter on this column" @click="addAsFilter">
						<i class="bi bi-funnel-fill me-1"></i> Add filter
					</button>
				</li>
				<li>
					<button
						type="button"
						class="dropdown-item"
						:class="!editableFilter ? 'disabled' : ''"
						:disabled="!editableFilter"
						:title="editableFilter ? 'Edit the existing filter on this column' : 'No single-leaf filter on this column to edit'"
						@click="editFilter"
					>
						<i class="bi bi-pencil-square me-1"></i> Edit filter
					</button>
				</li>
			</ul>
		</span>
	`,
};
