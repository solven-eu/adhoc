// @ts-check
import { reactive, ref, watch } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";
import { usePreferencesStore } from "./store-preferences.js";

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

import { collectCubeTags, defaultSelectedTags } from "./adhoc-baked-in-tags.js";

// wizardOptions are stored in localStorage as they should not be shared by URL, as they are User-preferences
const loadOptionsFromStorage = function () {
	return JSON.parse(localStorage.getItem("adhoc.preferences.wizardOptions")) || {};
};
const saveOptionsToStorage = function (options) {
	localStorage.setItem("adhoc.preferences.wizardOptions", JSON.stringify(options));
};

// In case of new verion of corrupted storage, we may need to add/remove/migrate some options
const sanitizeOptions = function (options) {
	if (!(typeof options.text === "string")) {
		options.text = "";
	}

	if (!(typeof options.caseSensitive === "boolean")) {
		// By default, not case-sensitive
		// Else, a user not seeing a match may be confused
		// While a user wanting case-sentitive can get more easily he has to click the toggle
		options.caseSensitive = false;
	}

	if (!(typeof options.throughJson === "boolean")) {
		// By default, we search along the names and the JSON
		// This is useful to report measures by some of their defintition like som filter
		// It may laos be problematic (e.g. searching a measure would report the measures depending on it)
		options.throughJson = true;
	}

	if (!Array.isArray(options.tags)) {
		// Tags can be focused by being added to this list
		options.tags = [];
	}

	return options;
};

const initOptions = function () {
	const optionsFromStorage = loadOptionsFromStorage();

	return sanitizeOptions(optionsFromStorage);
};

// A first visit has nothing in localStorage. Used to decide whether Pivotable may pick the cube's default tags: on any
// later visit the stored selection is the user's own, and must not be overridden.
const isFirstVisit = function () {
	return Object.keys(loadOptionsFromStorage()).length === 0;
};

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
		// Personal-history score maps: `{ columns: Map<name, score>, measures: Map<name, score>,
		// filterColumns: Map<name, score> }`. Each map ranks names the user has touched in past
		// successful queries on this cube. Consumed by the accordion components below to bias
		// `wizardHelper.filtered` toward personally-frequented entries within their match tier.
		// Required so a missing wiring is caught at mount time rather than silently degrading
		// to the alphabetical fallback.
		historyScores: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching", "nbColumnFetching"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				const self = /** @type {any} */ (this);
				return store.endpoints[self.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				const self = /** @type {any} */ (this);
				return store.schemas[self.endpointId] || { error: "not_loaded" };
			},
			cube(store) {
				const self = /** @type {any} */ (this);
				return store.schemas[self.endpointId]?.cubes[self.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();
		const preferencesStore = usePreferencesStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.endpointId);

		const firstVisit = isFirstVisit();
		const searchOptions = reactive(initOptions());

		// On a first visit, open on the cube's `essential` subset rather than on its full measure and column list,
		// which is unreadable on a large cube. Applied once the schema has loaded, since the cube has to be inspected
		// for the tag: defaulting to a tag no item carries would filter the wizard down to nothing.
		// The selection lands in `searchOptions.tags`, so it shows in the Tags dropdown and clears like any other.
		if (firstVisit) {
			const stopDefaulting = watch(
				() => store.schemas[props.endpointId]?.cubes[props.cubeId],
				(cube) => {
					if (!cube || cube.error) {
						return;
					}
					searchOptions.tags.push(...defaultSelectedTags(collectCubeTags(cube)));
					stopDefaulting();
				},
				{ immediate: true },
			);
		}

		// persist the whole state to the local storage whenever it changes
		watch(
			searchOptions,
			(searchOptions) => {
				saveOptionsToStorage(searchOptions);
			},
			{ deep: true },
		);

		return {
			searchOptions,
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
				<!--
					Three stacked sections with discrete visual separation so the user can tell at
					a glance where "query definition" (Filter) ends and "UI-side navigation aids"
					(Search, Tags) begin. Tiny uppercase muted labels act as section titles
					without eating vertical space.
				-->
				<section v-if="queryModel.filter" class="mb-2">
					<div class="text-uppercase text-muted small fw-semibold mb-1">Filter</div>
					<AdhocQueryWizardFilter :filter="queryModel.filter" />
				</section>

				<hr class="my-2" />

				<section>
					<div class="text-uppercase text-muted small fw-semibold mb-1">Search</div>
					<AdhocQueryWizardSearch :searchOptions="searchOptions" />
					<AdhocWizardTags :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" />
				</section>

				<div class="accordion" id="accordionWizard">
					<AdhocAccordionItemColumns
						:cubeId="cubeId"
						:endpointId="endpointId"
						:searchOptions="searchOptions"
						:columns="cube.columns.columns"
						:historyScores="historyScores.columns"
					/>
					<AdhocAccordionItemMeasures
						:cubeId="cubeId"
						:endpointId="endpointId"
						:searchOptions="searchOptions"
						:measures="cube.measures"
						:historyScores="historyScores.measures"
					/>

					<AdhocAccordionItemCustoms :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :customMarkers="cube.customMarkers" />
					<AdhocAccordionItemOptions :cubeId="cubeId" :endpointId="endpointId" :searchOptions="searchOptions" :options="{}" />
				</div>
			</form>
		</div>
	`,
};
