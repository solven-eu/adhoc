import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, {
			metadata(store) {
				return store.metadata;
			},
		}),
	},
	setup(props) {
		if (!props.queryModel.selectedOptions) {
			props.queryModel.selectedOptions = {};
		}

		return {};
	},
	template: /* HTML */ `
        <ul v-for="(option) in metadata.query_options" class="list-group list-group-flush">
            <li class="list-group-item">
                <div class="form-check form-switch">
                    <input class="form-check-input" type="checkbox" role="switch" :id="'option_' + option.name" v-model="queryModel.selectedOptions[option.name]" />
                    <label class="form-check-label text-wrap text-lowercase" :for="'option_' + option.name">{{option.name}}</label>
                </div>
            </li>
        </ul>
    `,
};
