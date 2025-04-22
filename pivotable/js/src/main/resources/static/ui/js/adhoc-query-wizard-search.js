import { computed, reactive, ref, watch, onMounted, inject } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import { useUserStore } from "./store-user.js";

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
		const store = useAdhocStore();
		const userStore = useUserStore();
		
		return {};
	},
	template: /* HTML */ `
	<div>
	    <input class="form-control mr-sm-2" type="search" placeholder="Search" aria-label="Search" id="search" v-model="searchOptions.text" />
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

	    <small v-for="tag in searchOptions.tags" class="badge text-bg-primary" @click="removeTag(tag)">
	        {{tag}} <i class="bi bi-x-circle"></i>
	    </small>
	</div>
    `,
};
