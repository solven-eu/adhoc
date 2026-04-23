// Used for debouncing on search
import _ from "lodashEs";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		// AdhocQueryWizardFilter,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	// emits: ['removeFilter'],
	setup(props) {
		// Debouncing `text` as the UI may before unresponsive while typing
		// UI shows `text_debounced`, which waits for some delay before updating the actual `text`
		// https://lodash.com/docs/4.17.15#debounce
		const onSearchedText = _.debounce(() => {
			console.log("Debounded searchedText", props.searchOptions.text);
			props.searchOptions.text = props.searchOptions.text_debounced;
		}, 300);

		const removeTag = function (tag) {
			return wizardHelper.removeTag(props.searchOptions, tag);
		};

		return { onSearchedText, removeTag };
	},
	template: /* HTML */ `
		<!--
			Compact search section. Single row: input-group with leading search icon; three
			switches (case-sensitive, JSON body search, queried-only) packed inline below with
			flex gap so they read as one control cluster. Selected tag chips render on the same
			line and are dismissed by click.
		-->
		<div class="mb-2">
			<div class="input-group input-group-sm">
				<span class="input-group-text py-0 px-2"><i class="bi bi-search small"></i></span>
				<input
					class="form-control"
					type="search"
					placeholder="Search"
					aria-label="Search"
					id="search"
					v-model="searchOptions.text_debounced"
					@input="onSearchedText"
				/>
			</div>

			<div class="d-flex flex-wrap gap-3 small mt-1">
				<div class="form-check form-switch mb-0" title="Case-sensitive search">
					<input class="form-check-input" type="checkbox" role="switch" id="searchCaseSensitive" v-model="searchOptions.caseSensitive" />
					<label class="form-check-label" for="searchCaseSensitive">Aa</label>
				</div>
				<div class="form-check form-switch mb-0" title="Also match on the JSON body of measures / columns">
					<input class="form-check-input" type="checkbox" role="switch" id="searchJson" v-model="searchOptions.throughJson" />
					<label class="form-check-label" for="searchJson">JSON</label>
				</div>
				<div class="form-check form-switch mb-0" title="Only show items currently included in the query">
					<input class="form-check-input" type="checkbox" role="switch" id="searchQuery" v-model="searchOptions.filterQueried" />
					<label class="form-check-label" for="searchQuery">Queried</label>
				</div>
			</div>

			<div v-if="searchOptions.tags.length" class="d-flex flex-wrap gap-1 mt-1">
				<span v-for="tag in searchOptions.tags" type="button" class="badge text-bg-primary" @click="removeTag(tag)">
					{{tag}} <i class="bi bi-x-circle"></i>
				</span>
			</div>
		</div>
	`,
};
