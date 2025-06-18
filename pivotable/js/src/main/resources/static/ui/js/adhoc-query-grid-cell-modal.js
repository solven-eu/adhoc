export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
		clickedCell: {
			type: Object,
			required: true,
		},
		cube: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		const applyEqualsFilter = function (column, coordinate) {
			// BEWARE This is poor design. We should send some event  managing the queryModel/filters
			if (!props.queryModel.filter || !props.queryModel.filter.type) {
				props.queryModel.filter = {};
				props.queryModel.filter.type = "and";
				props.queryModel.filter.filters = [];
			} else if (props.queryModel.filter.type !== "and") {
				throw new Error("We support only 'and'");
			}

			const columnFilter = { type: "column", column: column, valueMatcher: coordinate };
			props.queryModel.filter.filters.push(columnFilter);
			console.log("Added filter", columnFilter);
		};

		const columnIsFilterable = function (column) {
			return props.queryModel.selectedColumnsOrdered.includes(column);
		};

		const getUnderlyingsIfMeasure = function (column) {
			if (Object.keys(props.cube.measures).includes(column)) {
				const measure = props.cube.measures[column];

				const underlyings = [];

				if (measure.underlying) {
					underlyings.push(measure.underlying);
				} else if (measure.underlyings) {
					underlyings.push(...measure.underlyings);
				}

				return underlyings;
			} else {
				return [];
			}
		};

		const addMeasure = function (measure) {
			// TODO This should be a toggle
			props.queryModel.selectedMeasures[measure] = true;
		};

		return {
			applyEqualsFilter,
			columnIsFilterable,
			getUnderlyingsIfMeasure,
			addMeasure,
		};
	},
	template: /* HTML */ `
        <div class="modal fade" id="cellModal" tabindex="-1" aria-labelledby="cellModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="cellModalLabel">Cell Filter Editor</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <ul>
                            <li v-for="(coordinate, column) in clickedCell">
                                {{ column }}: {{ coordinate }}
                                <span v-if="columnIsFilterable(column)">
                                    <button type="button" class="btn" @click="applyEqualsFilter(column, coordinate)">
                                        <i class="bi bi-filter-circle"></i>
                                    </button>
                                </span>
                                <ul v-if="getUnderlyingsIfMeasure(column)">
                                    <li v-for="underlying in getUnderlyingsIfMeasure(column)">
                                        <button type="button" class="btn" @click="addMeasure(underlying)">
                                            {{underlying}} <i class="bi bi-plus-circle"></i>
                                        </button>
                                    </li>
                                </ul>
                            </li>
                        </ul>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" data-bs-dismiss="modal">Ok</button>
                    </div>
                </div>
            </div>
        </div>
    `,
};
