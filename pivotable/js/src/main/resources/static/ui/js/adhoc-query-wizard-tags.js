import { reactive, ref } from "vue";

import { useAdhocStore } from "./store-adhoc.js";

import AdhocQueryWizardMeasureTag from "./adhoc-query-wizard-measure-tag.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardMeasureTag,
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

		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		const store = useAdhocStore();

		const availableTags = function () {
			// https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Set
			const tags = new Set();

			const cube = store.schemas[this.endpointId]?.cubes[this.cubeId];
			for (const measure of Object.values(cube.measures)) {
				for (const tag of measure.tags) {
					tags.add(tag);
				}
			}
			for (const column of Object.values(cube.columns.columns)) {
				for (const tag of column.tags) {
					tags.add(tag);
				}
			}

			// https://stackoverflow.com/questions/20069828/how-to-convert-set-to-array
			const asArray = Array.from(tags);
			asArray.sort();
			return asArray;
		};

		return {
			availableTags,
		};
	},
	template: /* HTML */ `
        <div>
            <div class="dropdown">
                <button class="btn btn-secondary dropdown-toggle btn-sm" type="button" data-bs-toggle="dropdown" aria-expanded="false">
                    Tags
                    <span v-if="searchOptions.tags.length == 0"> {{availableTags().length}} </span>
                    <span v-else> {{searchOptions.tags.length}} / {{availableTags().length}} </span>
                </button>
                <ul class="dropdown-menu">
                    <li class="dropdown-item" v-for="tag in availableTags()">
                        <AdhocQueryWizardMeasureTag :tag="tag" :searchOptions="searchOptions" />
                    </li>
                </ul>
            </div>
        </div>
    `,
};
