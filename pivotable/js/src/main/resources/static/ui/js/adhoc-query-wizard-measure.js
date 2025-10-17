import { ref, inject } from "vue";

import { markMatchingWizard } from "./adhoc-query-wizard-search-helpers.js";

import AdhocQueryWizardMeasureTag from "./adhoc-query-wizard-measure-tag.js";

import { Modal } from "bootstrap";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardMeasureTag,
	},
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

		const refDagModal = ref(null);
		const measuresDagModel = inject("measuresDagModel");

		const toggleInfo = function () {
			if (!refDagModal.value) {
				let measureDagModal = new Modal(document.getElementById("measureDag"), {});
				refDagModal.value = measureDagModal;
			}

			measuresDagModel.main = props.measure.name;
			refDagModal.value.show();
		};

		const filteredEntry = function (measure) {
			const filteredCopy = Object.assign({}, measure);

			// Name is shown as header: no need to show it again in details
			delete filteredCopy.name;
			// Unclear why we receive a `key` which duplicate the `name`
			delete filteredCopy.key;
			// tags are shown as badges
			delete filteredCopy.tags;

			return filteredCopy;
		};

		return { mark, toggleInfo, filteredEntry };
	},
	template: /* HTML */ `
        <span v-html="mark(measure.name)" />
        <span type="button" :class="'badge text-bg-' + 'primary'" @click.prevent="toggleInfo()">
            <span>?</span>
        </span>
        &nbsp;
        <AdhocQueryWizardMeasureTag v-for="tag in measure.tags" :tag="tag" :searchOptions="searchOptions" />
        <div v-if="showDetails" class="text-muted">
            <span v-if="measure.type == '.Aggregator' ">
                <small v-html="mark(measure.aggregationKey + '(' + measure.columnName + ')')" />
            </span>
            <span v-else-if="measure.type == 'eu.solven.adhoc.table.composite.SubMeasureAsAggregator'">
                <small v-html="mark(measure.aggregationKey + '(' + measure.subMeasure + ')')" />
            </span>
            <span v-else-if="measure.type == '.Combinator'">
                <small v-html="mark(measure.combinationKey + '(' + measure.underlyings.join(', ') + ')')" />
            </span>
            <span v-else-if="measure.type == '.Dispatchor'">
                <small v-html="'dispatching by ' + mark(measure.decompositionKey + '(' + measure.underlying + ')')" />
            </span>
            <small v-else-if="measure.type == '.Filtrator'">
                <ul>
                    <li>underlying: <span v-html="mark(measure.underlying)"></span></li>
                    <li>key: <span v-html="mark(measure.key)"></span></li>
                    <li>tags: <span v-html="mark(measure.tags)"></span></li>
                    <li>filter: <span v-html="mark(measure.filter)"></span></li>
                </ul>
            </small>
            <small v-else>
                <ul>
                    <li v-for="(value, key) in filteredEntry(measure)">{{key}}: <span v-html="mark(value)"></span></li>
                </ul>
            </small>
        </div>
    `,
};
