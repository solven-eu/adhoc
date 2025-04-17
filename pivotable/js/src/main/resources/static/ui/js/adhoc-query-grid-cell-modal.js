import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import { useUserStore } from "./store-user.js";

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
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
	},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

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

		return {
			applyEqualsFilter,
			columnIsFilterable,
		};
	},
	template: /* HTML */ `
        <div class="modal fade" id="cellModal" tabindex="-1" aria-labelledby="cellModalLabel" aria-hidden="true">
            <div class="modal-dialog">
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
                                    <button type="button" class="btn btn-primary" @click="applyEqualsFilter(column, coordinate)">
                                        Filter {{column}}={{coordinate}}
                                    </button>
                                </span>
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
