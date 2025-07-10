import { reactive, ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardSearch from "./adhoc-query-wizard-search.js";
import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";
import AdhocQueryWizardFilter from "./adhoc-query-wizard-filter.js";

import AdhocQueryWizardMeasureTag from "./adhoc-query-wizard-measure-tag.js";

import AdhocAccordionItemColumns from "./adhoc-query-wizard-accordion-columns.js";
import AdhocAccordionItemMeasures from "./adhoc-query-wizard-accordion-measures.js";
import AdhocAccordionItemCustoms from "./adhoc-query-wizard-accordion-customs.js";
import AdhocAccordionItemOptions from "./adhoc-query-wizard-accordion-options.js";

import AdhocWizardTags from "./adhoc-query-wizard-tags.js";

import { useUserStore } from "./store-user.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardSearch,
		AdhocQueryWizardColumn,
		AdhocQueryWizardFilter,
		AdhocQueryWizardMeasureTag,

		AdhocAccordionItemColumns,
		AdhocAccordionItemMeasures,
		AdhocAccordionItemCustoms,
		AdhocAccordionItemOptions,

		AdhocWizardTags,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		endpointId: {
			type: String,
			required: true,
		},

		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching", "nbColumnFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				return store.schemas[this.endpointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const searchOptions = reactive({
			text: "",

			// By default, not case-sensitive
			// Else, a user not seeing a match may be confused
			// While a user wanting case-sentitive can get more easily he has to click the toggle
			caseSensitive: false,

			// By default, we search along the names and the JSON
			// This is useful to report measures by some of their defintition like som filter
			// It may laos be problematic (e.g. searching a measure would report the measures depending on it)
			throughJson: true,

			// Tags can be focused by being added to this list
			tags: [],
		});

		const filtered = function (arrayOrObject) {
			return wizardHelper.filtered(searchOptions, arrayOrObject);
		};
		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

		const removeTag = function (tag) {
			return wizardHelper.removeTag(searchOptions, tag);
		};

		const clearFilters = function () {
			return wizardHelper.clearFilters(searchOptions);
		};

		return {
			searchOptions,
			filtered,
			queried,
			removeTag,
			clearFilters,
		};
	},
	template: /* HTML */ `
        <div v-if="(!endpoint || !cube)">
            <div v-if="(nbSchemaFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="endpoint.error || cube.error">{{endpoint.error || cube.error}}</div>
        <div v-else>
            <form class="text-break">
                <AdhocQueryWizardFilter :filter="queryModel.filter" v-if="queryModel.filter" />
                <AdhocQueryWizardSearch :searchOptions="searchOptions" />

                <AdhocWizardTags :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" />

                <div class="accordion" id="accordionWizard">
                    <AdhocAccordionItemColumns :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :columns="cube.columns.columns" />
                    <AdhocAccordionItemMeasures :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :measures="cube.measures" />

                    <AdhocAccordionItemCustoms :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :customMarkers="cube.customMarkers" />
                    <AdhocAccordionItemOptions :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :options="{}" />
                </div>
            </form>
        </div>
    `,
};
