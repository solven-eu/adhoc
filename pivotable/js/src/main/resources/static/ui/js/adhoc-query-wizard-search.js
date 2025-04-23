import { computed } from "vue";

import _ from "lodashEs";

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

		return { onSearchedText };
	},
	template: /* HTML */ `
        <div>
            <input
                class="form-control mr-sm-2"
                type="search"
                placeholder="Search"
                aria-label="Search"
                id="search"
                v-model="searchOptions.text_debounced"
                @input="onSearchedText"
            />
            <small>
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" role="switch" id="searchCaseSensitive" v-model="searchOptions.caseSensitive" />
                    <label class="form-check-label" for="searchCaseSensitive">Aa</label>
                </div>
            </small>
            <small>
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" role="switch" id="searchJson" v-model="searchOptions.throughJson" />
                    <label class="form-check-label" for="searchJson">JSON</label>
                </div>
            </small>

            <small v-for="tag in searchOptions.tags" class="badge text-bg-primary" @click="removeTag(tag)"> {{tag}} <i class="bi bi-x-circle"></i> </small>
        </div>
    `,
};
