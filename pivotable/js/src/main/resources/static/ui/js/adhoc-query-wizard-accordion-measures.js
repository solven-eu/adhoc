import { ref, inject } from "vue";

import AdhocMeasure from "./adhoc-query-wizard-measure.js";
import AdhocQueryWizardColumn from "./adhoc-query-wizard-column.js";

import wizardHelper from "./adhoc-query-wizard-helper.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocMeasure,
		AdhocQueryWizardColumn,
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

		measures: {
			type: Object,
			required: true,
		},

		searchOptions: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		const queryModel = inject("queryModel");

		const filtered = function (arrayOrObject) {
			return wizardHelper.filtered(props.searchOptions, arrayOrObject, queryModel);
		};
		const queried = function (arrayOrObject) {
			return wizardHelper.queried(arrayOrObject);
		};

		const clearFilters = function () {
			return wizardHelper.clearFilters(props.searchOptions);
		};

		// Local toggle — controls the grey description line rendered under each measure name.
		// Default ON to preserve existing behaviour; users who want a denser list can flip it off.
		// Kept local (not in searchOptions) because it's a pure UI preference with no side effects
		// on search semantics.
		const showMeasureDetails = ref(true);

		return {
			filtered,
			queried,
			clearFilters,
			queryModel,
			showMeasureDetails,
		};
	},
	template: /* HTML */ `
		<div class="accordion-item">
			<h2 class="accordion-header">
				<button
					class="accordion-button collapsed"
					type="button"
					data-bs-toggle="collapse"
					data-bs-target="#wizardMeasures"
					aria-expanded="false"
					aria-controls="wizardMeasures"
				>
					<span v-if="searchOptions.text || searchOptions.tags.length > 0">
						<span class="text-decoration-line-through"> {{ Object.keys(measures).length}} </span>&nbsp;
						<span> {{ Object.keys(filtered(measures)).length}} </span> measures
					</span>
					<span v-else> {{ Object.keys(measures).length}} measures </span>&nbsp;
					<small class="badge text-bg-primary">{{queried(queryModel.selectedMeasures).length}}</small>
				</button>
			</h2>
			<div id="wizardMeasures" class="accordion-collapse collapse" data-bs-parent="#accordionWizard">
				<div class="accordion-body vh-50 overflow-scroll px-0">
					<!--
						UI preference toggle: hide / show the grey description line under each measure.
						Useful when the list is long and the user just wants to scan names.
						Right-aligned via flex (justify-content-end) so the group visually separates
						from the measure rows below (each measure has its own left-aligned switch),
						and kept with standard form-check layout so Bootstrap's 1.5em internal
						padding doesn't push the switch off the right edge of the narrow sidebar.
					-->
					<div class="d-flex justify-content-end pe-3 mb-1">
						<div class="form-check form-switch mb-0">
							<input class="form-check-input" type="checkbox" role="switch" id="showMeasureDetails" v-model="showMeasureDetails" />
							<label class="form-check-label text-muted small" for="showMeasureDetails">Show descriptions</label>
						</div>
					</div>

					<ul v-for="(measure) in filtered(measures)" class="list-group list-group-flush">
						<li class="list-group-item d-flex align-items-center gap-2">
							<div
								class="form-check form-switch flex-grow-1"
								:class="queryModel.disabledMeasures &amp;&amp; queryModel.disabledMeasures[measure.name] ? 'opacity-50' : ''"
							>
								<input
									class="form-check-input"
									type="checkbox"
									role="switch"
									:id="'measure_' + measure.name"
									v-model="queryModel.selectedMeasures[measure.name]"
								/>
								<label
									class="form-check-label"
									:class="queryModel.disabledMeasures &amp;&amp; queryModel.disabledMeasures[measure.name] ? 'text-decoration-line-through' : ''"
									:for="'measure_' + measure.name"
								>
									<AdhocMeasure :measure="measure" :showDetails="showMeasureDetails" :searchOptions="searchOptions" />
								</label>
							</div>
							<!--
								Pause / resume toggle. Always rendered so the affordance is
								discoverable; muted + disabled until the measure is picked. Mirrors
								the icon and tooltip wording used by the filter tree's disable
								button, so the affordance is identical across measures, columns,
								and filters.
							-->
							<button
								type="button"
								class="btn btn-sm btn-link p-0 text-decoration-none"
								:class="queryModel.selectedMeasures[measure.name] ? '' : 'opacity-25'"
								:disabled="!queryModel.selectedMeasures[measure.name]"
								:title="!queryModel.selectedMeasures[measure.name] ? 'Pick this measure first to enable pause' : ((queryModel.disabledMeasures &amp;&amp; queryModel.disabledMeasures[measure.name]) ? 'Resume this measure' : 'Pause this measure (keep in model, skip at query time)')"
								@click.stop="queryModel.disabledMeasures &amp;&amp; (queryModel.disabledMeasures[measure.name] = !queryModel.disabledMeasures[measure.name])"
							>
								<i
									:class="(queryModel.disabledMeasures &amp;&amp; queryModel.disabledMeasures[measure.name]) ? 'bi bi-play-circle' : 'bi bi-pause-circle'"
								></i>
							</button>
						</li>
					</ul>

					<span v-if="0 === filtered(measures).length">
						Search options match no column. <button type="button" class="btn btn-secondary" @click="clearFilters">clearFilters</button>
					</span>
				</div>
			</div>
		</div>
	`,
};
