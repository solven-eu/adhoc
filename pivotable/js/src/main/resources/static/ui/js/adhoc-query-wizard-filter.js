import { computed, reactive, ref, watch, onMounted } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

import { useUserStore } from "./store-user.js";

// import AdhocQueryWizardFilter from "./adhoc-query-wizard-filter.js";

export default {
	name: "AdhocQueryWizardFilter",
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		// AdhocQueryWizardFilter,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		filter: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		const store = useAdhocStore();
		const userStore = useUserStore();

		return {};
	},
	template: /* HTML */ `
        <div v-if="filter.type === 'and'">
            <span v-for="(operand, index) in filter.filters"> <span v-if="index !== 0">&amp;&amp;</span> <AdhocQueryWizardFilter :filter="operand" /> </span>
        </div>
        <div v-else-if="filter.type === 'or'">
            <span v-for="(operand, index) in filter.filters"> <span v-if="index !== 0">||</span> <AdhocQueryWizardFilter :filter="operand" /> </span>
        </div>
        <span v-else-if="filter.type==='column'"> {{filter.column}}={{filter.valueMatcher}} </span>
        <div v-else>{{filter}}</div>
    `,
};
