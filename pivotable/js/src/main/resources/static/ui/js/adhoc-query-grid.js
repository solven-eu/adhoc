import { ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocMeasure from "./adhoc-measure.js";

import { useUserStore } from "./store-user.js";

import { SlickGrid, SlickDataView, Formatters } from "slickgrid";
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

		// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
		const numberFormat = new Intl.NumberFormat({ maximumSignificantDigits: 3 });

		// https://github.com/6pac/SlickGrid/blob/master/src/slick.formatters.ts
		function percentFormatter(row, cell, value, columnDef, dataContext) {
			var rtn = {};

			rtn.text = numberFormat.format(value * 100) + "%";
			rtn.toolTip = value;

			return rtn;
		}

		const rendering = ref(false);

		function renderSparkline(cellNode, row, dataContext, colDef) {
			rendering.value = false;
		}

		// https://github.com/6pac/SlickGrid/wiki/Providing-data-to-the-grid
		let resyncData = function () {
			const view = props.tabularView.value;

			// https://stackoverflow.com/questions/1232040/how-do-i-empty-an-array-in-javascript
			columns = [];
			data = [];

			rendering.value = true;

			if (!view.coordinates) {
				const column = { id: "empty", name: "empty", field: "empty", sortable: true, asyncPostRender: renderSparkline };

				columns.push(column);
				data.push({ id: "0", empty: "empty" });
			} else {
				// https://github.com/6pac/SlickGrid/blob/master/examples/example-grouping-esm.html
				for (let i = 0; i < view.coordinates.length; i++) {
					const coordinatesRow = view.coordinates[i];
					const measuresRow = view.values[i];

					if (i == 0) {
						for (const property of Object.keys(coordinatesRow)) {
							const column = { id: property, name: property, field: property, sortable: true, asyncPostRender: renderSparkline };
							columns.push(column);
						}
						for (const property of Object.keys(measuresRow)) {
							const column = { id: property, name: property, field: property, sortable: true, asyncPostRender: renderSparkline };

							if (property.indexOf("%") >= 0) {
								column["formatter"] = percentFormatter;
							}

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

			// https://stackoverflow.com/questions/12128680/slickgrid-what-is-a-data-view
			//grid.setData(data);
			dataView.setItems(data);

			dataView.refresh();
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

		// https://github.com/6pac/SlickGrid/wiki/Grid-Options
		let options = {
			enableColumnReorder: true,
			enableAutoSizeColumns: true,
			//			autoHeight: true,
			fullWidthRows: true,
			forceFitColumns: true,
			// https://github.com/6pac/SlickGrid/blob/master/examples/example10-async-post-render.html		,
			enableAsyncPostRender: true,
		};

		// Use AutoResizer?
		// https://6pac.github.io/SlickGrid/examples/example15-auto-resize.html

		onMounted(() => {
			// SlickGrid requires the DOM to be ready: `onMounted` is needed
			grid = new SlickGrid("#" + props.domId, dataView, columns, options);
			dataView.refresh();

			// TODO comparer function is never called?
			function comparer(a, b) {
				let x = a[sortcol],
					y = b[sortcol];
				return x == y ? 0 : x > y ? 1 : -1;
			}

			let sortdir = 1;
			let sortcol = "unknown";
			grid.onSort.subscribe(function (e, args) {
				sortdir = args.sortAsc ? 1 : -1;
				sortcol = args.sortCol.field;

				// using native sort with comparer
				// preferred method but can be very slow in IE with huge datasets
				dataView.sort(comparer, args.sortAsc);
			});
		});

		return { rendering };
	},
	template: /* HTML */ `
		<div>
			<div class="spinner-grow" role="status" v-if="loading">
			  <span class="visually-hidden">Loading...</span>
			</div>
			
			<!--div v-for="(row, index) in tabularView.value?.coordinates">
				{{row}} -> {{tabularView.value.values[index]}}
			</div-->
			
			rendering = {{rendering}}
			<div>
			  <div class="grid-header" style="width:100%;">
			    <label>SlickGrid</label>
			  </div>
			  <div :id="domId" style="width:100%;height:500px;"></div>
			</div>
        </div>
    `,
};
