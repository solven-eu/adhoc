import { ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

import { SlickGrid, SlickDataView } from "slickgrid";
import Sortable from "sortablejs";

// https://github.com/SortableJS/Sortable/issues/1229#issuecomment-521951729
window.Sortable = Sortable;

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		domId: {
			type: String,
			required: true,
		},
		tabularView: {
			type: Object,
			required: true,
		},
		loading: {
			type: Boolean,
			default: false,
		},
	},
	setup(props) {
		// https://stackoverflow.com/questions/2402953/javascript-data-grid-for-millions-of-rows
		let dataView;

		let grid;
		let data = [];

		let columns = [];

		let resyncData = function () {
			const view = props.tabularView.value;

			// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
			columns = [];
			data.length = 0;

			if (!view.coordinates) {
				const column = { id: "empty", name: "empty", field: "empty", sortable: true };
				columns.push(column);
				data.push({ id: "0", empty: "empty" });
			} else {
				// https://github.com/6pac/SlickGrid/blob/master/examples/example-grouping-esm.html
				for (let i = 0; i < view.coordinates.length; i++) {
					const coordinatesRow = view.coordinates[i];
					const measuresRow = view.values[i];

					if (i == 0) {
						for (const property of Object.keys(coordinatesRow)) {
							const column = { id: property, name: property, field: property, sortable: true };
							columns.push(column);
						}
						for (const property of Object.keys(measuresRow)) {
							const column = { id: property, name: property, field: property, sortable: true };
							columns.push(column);
						}
					}

					let d = {};

					d["id"] = i;

					for (const property of Object.keys(coordinatesRow)) {
						d[property] = coordinatesRow[property];
					}
					for (const property of Object.keys(measuresRow)) {
						d[property] = measuresRow[property];
					}

					data.push(d);
				}
			}

			grid.setColumns(columns);
		};

		watch(
			() => props.tabularView,
			() => {
				resyncData();
				// ???
				dataView.refresh();
				// ???
				grid.invalidate();
			},
			{ deep: true },
		);

		dataView = new SlickDataView({});

		dataView.setItems(data);

		let options = {
			rowHeight: 28,
		};

		onMounted(() => {
			// SlickGrid requires the DOM to be ready: `onMounted` is needed
			grid = new SlickGrid("#" + props.domId, dataView, columns, options);
			dataView.refresh();
		});

		// dataView.refresh();
		// grid.invalidate();

		return {};
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
			  <span class="visually-hidden">Loading...</span>
			</div>
			
			<div v-for="(row, index) in tabularView.value?.coordinates">
				{{row}} -> {{tabularView.value.values[index]}}
			</div>
			
			<div style="width:600px;">
			  <div class="grid-header" style="width:100%">
			    <label>SlickGrid</label>
			  </div>
			  <div :id="domId" style="width:100%;height:500px;"></div>
			</div>
        </div>
    `,
};
