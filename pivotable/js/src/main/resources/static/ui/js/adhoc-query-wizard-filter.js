import { inject } from "vue";

export default {
	name: "AdhocQueryWizardFilter",
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		filter: {
			type: Object,
			required: true,
		},
		path: {
			type: Array,
			default: [],
		},
	},
	computed: {},
	// emits: ['removeFilter'],
	setup(props) {
		const childrenPath = function (subComponent) {
			let newArray = props.path.slice();

			newArray.push(subComponent);

			return newArray;
		};

		const queryModel = inject("queryModel");

		const removeFilter = function () {
			// A NOT wrapper has a single child under `.negated`. Semantically, "remove the
			// inner of a NOT" is the same as "remove the whole NOT" — a NOT with no operand
			// is meaningless — so we collapse trailing `negated` path segments to their
			// parent position before running the standard remove logic.
			let effectivePath = props.path.slice();
			while (effectivePath.length > 0 && effectivePath[effectivePath.length - 1] === "negated") {
				effectivePath.pop();
			}

			// Start drilling from the root
			let filterSubObject = queryModel.filter;

			const pathLength = effectivePath.length;

			if (pathLength == 0) {
				console.log("Clearing the whole filter");
				queryModel.filter = {};
			} else {
				for (let pathIndex = 0; pathIndex < pathLength; pathIndex++) {
					if (!filterSubObject) {
						console.log("Drilled filter is empty");
						break;
					} else if (filterSubObject.type === "and" || filterSubObject.type === "or") {
						filterSubObject = filterSubObject.filters;
					} else if (filterSubObject.type === "not") {
						// NOT drills straight into its wrapped child.
						filterSubObject = filterSubObject.negated;
					}

					const pathComponent = effectivePath[pathIndex];

					if (pathIndex == pathLength - 1) {
						console.log("Removing", pathComponent, "from", filterSubObject);
						filterSubObject.splice(pathComponent, 1);
					} else {
						const drilledFilterSubObject = filterSubObject[pathComponent];
						console.log("Drilling for filter removal. ", pathComponent, filterSubObject, drilledFilterSubObject);
						filterSubObject = drilledFilterSubObject;
					}
				}

				// Collapse the top-level AND/OR to `{}` (matchAll) when removing the last
				// child emptied the group. Otherwise the sidebar would render an "empty AND"
				// pill — the render-time guard in the template also catches this, but the
				// model is cleaner without the stale `{type:'and', filters:[]}` husk.
				if (
					queryModel.filter &&
					(queryModel.filter.type === "and" || queryModel.filter.type === "or") &&
					Array.isArray(queryModel.filter.filters) &&
					queryModel.filter.filters.length === 0
				) {
					queryModel.filter = {};
				}
			}
		};

		// Toggle the `disabled` flag on THIS filter node. The flag is a Pivotable-side UI
		// preference: the filter stays in `queryModel.filter` (so the user sees it and can
		// flip it back on), but `adhoc-query-executor.js` strips disabled nodes before
		// sending the query to the backend. This is the beginning of a deliberate split
		// between the "pivotable model" (what the user has configured in the UI) and the
		// "query model" (what actually goes over the wire).
		const toggleDisabled = function () {
			// Mutate through the reactive queryModel tree so Vue picks up the change. `props.filter`
			// is the exact same object reference, so a direct `props.filter.disabled = ...` is
			// tracked — no need to re-walk the tree.
			props.filter.disabled = !props.filter.disabled;
		};

		return { childrenPath, removeFilter, toggleDisabled };
	},
	template: /* HTML */ `
		<!--
			Compact, pill-style filter tree. Each node renders inline with a small label
			badge (AND/OR/column) plus two icon buttons: toggle-enabled (eye) and remove (×).
			Nested AND/OR use a thin left border + reduced indent so deep trees stay readable
			in the narrow sidebar without drowning in <ul> padding.
		-->
		<span v-if="!filter" class="text-muted small">NULL?</span>

		<!--
			NOT wrapper: renders as a red NOT pill followed by the single child under
			filter.negated. Pause/remove buttons apply to the whole NOT; the child's own
			pause/remove inside it are semantically linked — disabling or removing the
			inner collapses the whole NOT (see removeFilter and stripDisabledFilters).
		-->
		<div v-else-if="filter.type === 'not'" class="d-inline-block" :class="filter.disabled ? 'text-muted' : ''">
			<span class="badge rounded-pill text-bg-danger me-1" :class="filter.disabled ? 'text-decoration-line-through opacity-50' : ''">NOT</span>
			<button
				type="button"
				class="btn btn-sm btn-link p-0 me-1 align-baseline"
				:title="filter.disabled ? 'Enable this NOT' : 'Disable this NOT (keep in model, skip at query time)'"
				@click="toggleDisabled"
			>
				<i :class="filter.disabled ? 'bi bi-play-circle' : 'bi bi-pause-circle'"></i>
			</button>
			<button type="button" class="btn btn-sm btn-link p-0 text-danger align-baseline" title="Remove" @click="removeFilter">
				<i class="bi bi-x-circle"></i>
			</button>
			<div class="ps-3 border-start small">
				<AdhocQueryWizardFilter :filter="filter.negated" :path="childrenPath('negated')" />
			</div>
		</div>

		<!--
			Empty AND with no children is semantically matchAll (empty conjunction is true).
			We branch to the matchAll pill BEFORE the generic AND/OR branch so the user
			sees the same infinity icon as the plain-{} case — this happens after removing
			the last filter from a group, when the save/remove paths leave
			{type:"and", filters:[]} in the model. (An empty OR is logically matchNone, but
			we treat it the same here: it should never occur in practice since OR groups are
			authored deliberately.)
		-->
		<span
			v-else-if="(filter.type === 'and' || filter.type === 'or') && (!filter.filters || filter.filters.length === 0)"
			class="badge rounded-pill text-bg-secondary opacity-75"
			title="No filter — matches all rows (matchAll)"
		>
			<i class="bi bi-infinity me-1"></i>matchAll
		</span>

		<div v-else-if="filter.type === 'and' || filter.type === 'or'" class="d-inline-block" :class="filter.disabled ? 'text-muted' : ''">
			<span
				class="badge rounded-pill me-1"
				:class="[filter.type === 'and' ? 'text-bg-primary' : 'text-bg-info', filter.disabled ? 'text-decoration-line-through opacity-50' : '']"
				>{{ filter.type.toUpperCase() }}</span
			>
			<button
				type="button"
				class="btn btn-sm btn-link p-0 me-1 align-baseline"
				:title="filter.disabled ? 'Enable this group' : 'Disable this group (keep in model, skip at query time)'"
				@click="toggleDisabled"
			>
				<i :class="filter.disabled ? 'bi bi-play-circle' : 'bi bi-pause-circle'"></i>
			</button>
			<button type="button" class="btn btn-sm btn-link p-0 text-danger align-baseline" title="Remove" @click="removeFilter">
				<i class="bi bi-x-circle"></i>
			</button>
			<ul class="list-unstyled ps-3 mb-0 small border-start">
				<li v-for="(operand, index) in filter.filters" class="ps-1">
					<AdhocQueryWizardFilter :filter="operand" :path="childrenPath(index)" />
				</li>
			</ul>
		</div>

		<span v-else-if="filter.type==='column'" class="d-inline-flex align-items-center gap-1" :class="filter.disabled ? 'text-muted' : ''">
			<span class="small" :class="filter.disabled ? 'text-decoration-line-through' : ''">
				<span class="fw-semibold">{{filter.column}}</span>
				<span class="text-muted">=</span>
				<span>{{filter.valueMatcher}}</span>
			</span>
			<button
				type="button"
				class="btn btn-sm btn-link p-0 align-baseline"
				:title="filter.disabled ? 'Enable this filter' : 'Disable this filter (keep in model, skip at query time)'"
				@click="toggleDisabled"
			>
				<i :class="filter.disabled ? 'bi bi-play-circle' : 'bi bi-pause-circle'"></i>
			</button>
			<button type="button" class="btn btn-sm btn-link p-0 text-danger align-baseline" title="Remove" @click="removeFilter">
				<i class="bi bi-x-circle"></i>
			</button>
		</span>

		<!--
			Empty filter object ({}) is the "no filter" state — semantically equivalent to
			the backend's matchAll filter, meaning every row is kept. Rendered as a muted
			pill with an infinity icon instead of the raw {} that Vue would otherwise emit
			via the generic {{filter}} fallback below.
		-->
		<span
			v-else-if="Object.keys(filter).length === 0"
			class="badge rounded-pill text-bg-secondary opacity-75"
			title="No filter — matches all rows (matchAll)"
		>
			<i class="bi bi-infinity me-1"></i>matchAll
		</span>

		<span v-else class="small text-muted">{{filter}}</span>
	`,
};
