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
			// ctx.emit('removeFilter', {path: props.path});

			// Start drilling from the root
			let filterSubObject = queryModel.filter;

			const pathLength = props.path.length;

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
					}

					const pathComponent = props.path[pathIndex];

					if (pathIndex == pathLength - 1) {
						console.log("Removing", pathComponent, "from", filterSubObject);
						// delete filterSubObject[pathComponent];
						filterSubObject.splice(pathComponent, 1);
					} else {
						const drilledFilterSubObject = filterSubObject[pathComponent];
						console.log("Drilling for filter removal. ", pathComponent, filterSubObject, drilledFilterSubObject);
						filterSubObject = drilledFilterSubObject;
					}
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

		<span v-else class="small text-muted">{{filter}}</span>
	`,
};
