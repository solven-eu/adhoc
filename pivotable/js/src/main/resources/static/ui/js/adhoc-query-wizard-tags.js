// @ts-check
import { reactive, ref } from "vue";

import { useAdhocStore } from "./store-adhoc.js";

import { toggleTag } from "./adhoc-query-wizard-tag-toggle.js";
import { collectCubeTags, describeTag } from "./adhoc-baked-in-tags.js";

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
			return collectCubeTags(store.schemas[props.endpointId]?.cubes[props.cubeId]);
		};

		// Tooltip explaining what a tag means. Cube-declared descriptions win over Pivotable's built-in knowledge;
		// until the cube description API carries them, only the baked-in tags resolve to anything.
		const tagDescription = function (tag) {
			const cube = store.schemas[props.endpointId]?.cubes[props.cubeId];
			return describeTag(tag, cube?.tagDescriptions);
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
			tagDescription,
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
						<!-- Description as a tooltip rather than a second line: the menu is already height-capped. -->
						<button type="button" class="dropdown-item" :title="tagDescription(tag)" @click.prevent="toggle(tag)">
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
