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
		
		const tagFilter = ref("");

		const availableTags = function () {
			// https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Set
			const tags = new Set();

			const cube = store.schemas[props.endpointId]?.cubes[props.cubeId];
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
			return Array.from(tags);
		};

		const filteredTags = function () {
			let asArray = availableTags();
			
			asArray = asArray.filter(e =>  {
				return e.indexOf(tagFilter.value) >= 0;
			});
			
			asArray.sort();
			return asArray;
		};

		return {
			availableTags,
			filteredTags,
			tagFilter,
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
					<li class="dropdown-item">
						<div class="mb-3">
						  <label for="tagsFilterInput" class="form-label">Filter tags in the Wizard</label>
						  <input type="text" class="form-control" id="tagsFilterInput" placeholder="Filter tags" v-model="tagFilter">
						</div>
					</li>
					<li><hr class="dropdown-divider"></li>
                    <li class="dropdown-item" v-for="tag in filteredTags()">
                        <AdhocQueryWizardMeasureTag :tag="tag" :searchOptions="searchOptions" />
                    </li>
					<li class="dropdown-item text-secondary">
						<small>{{availableTags().length - filteredTags().length }} tags are filtered out</small>
					</li>
                </ul>
            </div>
        </div>
    `,
};
