// @ts-check
import { reactive, ref } from "vue";

import { useAdhocStore } from "./store-adhoc.js";

import { toggleTag } from "./adhoc-query-wizard-tag-toggle.js";

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
				// tags may be empty in case of error column
				for (const tag of column.tags || []) {
					tags.add(tag);
				}
			}

			// https://stackoverflow.com/questions/20069828/how-to-convert-set-to-array
			return Array.from(tags);
		};

		const filteredTags = function () {
			let asArray = availableTags();

			asArray = asArray.filter((e) => {
				return e.indexOf(tagFilter.value) >= 0;
			});

			asArray.sort();
			return asArray;
		};

		const toggle = function (tag) {
			toggleTag(props.searchOptions.tags, tag);
		};

		return {
			availableTags,
			filteredTags,
			tagFilter,
			toggle,
		};
	},
	template: /* HTML */ `
		<div>
			<div class="dropdown">
				<!-- auto-close=outside keeps the menu open while several tags are toggled; it is a multi-select. -->
				<button
					class="btn btn-secondary dropdown-toggle btn-sm"
					type="button"
					data-bs-toggle="dropdown"
					data-bs-auto-close="outside"
					aria-expanded="false"
				>
					Tags
					<span v-if="searchOptions.tags.length == 0"> {{availableTags().length}} </span>
					<span v-else> {{searchOptions.tags.length}} / {{availableTags().length}} </span>
				</button>
				<!-- Capped height: a cube with many tags would otherwise run the menu past the bottom of the viewport. -->
				<ul class="dropdown-menu" style="max-height: 80vh; overflow-y: auto">
					<li class="dropdown-item">
						<div class="mb-3">
							<label for="tagsFilterInput" class="form-label">Filter tags in the Wizard</label>
							<input type="text" class="form-control" id="tagsFilterInput" placeholder="Filter tags" v-model="tagFilter" />
						</div>
					</li>
					<li><hr class="dropdown-divider" /></li>
					<!-- One tag per row, so the whole row toggles it rather than the badge alone being a target. -->
					<li v-for="tag in filteredTags()">
						<button type="button" class="dropdown-item" @click.prevent="toggle(tag)">
							<AdhocQueryWizardMeasureTag :tag="tag" :searchOptions="searchOptions" />
						</button>
					</li>
					<li class="dropdown-item text-secondary">
						<small>{{availableTags().length - filteredTags().length }} tags are filtered out</small>
					</li>
				</ul>
			</div>
		</div>
	`,
};
