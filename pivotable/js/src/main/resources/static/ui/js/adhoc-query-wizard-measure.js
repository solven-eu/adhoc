import {} from "vue";

import { markMatchingWizard } from "./adhoc-query-wizard-search-helpers.js";

export default {
	props: {
		measure: {
			type: Object,
			required: true,
		},
		showDetails: {
			type: Boolean,
			default: true,
		},
		searchOptions: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const mark = function (text) {
			return markMatchingWizard(props.searchOptions, text);
		};

		return { mark };
	},
	template: /* HTML */ `
        <span v-html="mark(measure.name)" />
        <span v-if="showDetails" class="text-muted">
            <span v-if="measure.type == '.Aggregator'">
                <small v-html="mark(measure.aggregationKey + '(' + measure.columnName + ')')" />
            </span>
            <span v-else-if="measure.type == '.Combinator'">
                <small v-html="mark(measure.combinationKey + '(' + measure.underlyings.join(', ') + ')')" />
            </span>
            <span v-else-if="measure.type == '.Dispatchor'">
                <small v-html="'dispatching by ' + mark(measure.decompositionKey + '(' + measure.underlyings.join(', ') + ')')" />
            </span>
            <span v-else-if="measure.type == '.Filtrator'">
                <small v-html="'filtering ' + mark(measure.filter + '(' + measure.underlying + ')')" />
            </span>
            <span v-else>
                <small v-html="mark(measure)"></small>
            </span>
        </span>
    `,
};
