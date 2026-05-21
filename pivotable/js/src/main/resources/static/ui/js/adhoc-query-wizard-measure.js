// @ts-check
import { ref, inject } from "vue";

import { markMatchingWizard } from "./adhoc-query-wizard-search-helpers.js";

import AdhocQueryWizardMeasureTag from "./adhoc-query-wizard-measure-tag.js";

import { Modal } from "bootstrap";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocQueryWizardMeasureTag,
	},
	props: {
		measure: {
			type: Object,
			required: true,
		},
		showDetails: {
			type: Boolean,
			default: true,
		},
		searchOptions: {
			type: Object,
			required: true,
		},
		// Personal-history weight. See the matching prop on adhoc-query-wizard-column.js — same
		// contract, same visual idiom. Wired through by the accordion-measures component from the
		// `_historyScore` stamped onto each measure entry by wizardHelper.filtered().
		historyScore: {
			type: Number,
			default: 0,
		},
		// Wizard-search waterfall score — same contract as the matching prop on
		// adhoc-query-wizard-column.js. When the active search produced a sub-100 match for this
		// measure, the score is forwarded so the row can render a small percentage badge. Default
		// undefined / 100 → no badge, matching the column variant.
		matchScore: {
			type: Number,
			default: undefined,
		},
		// True when this row was surfaced via the drop-tags fallback (same as on the column
		// component). Renders a distinct chip so the user knows tags were ignored.
		matchTagsBypassed: {
			type: Boolean,
			default: false,
		},
	},
	setup(props) {
		const mark = function (text) {
			return markMatchingWizard(props.searchOptions, text);
		};

		const refDagModal = ref(null);
		const measuresDagModel = inject("measuresDagModel");

		const toggleInfo = function () {
			if (!refDagModal.value) {
				let measureDagModal = new Modal(document.getElementById("measureDag"), {});
				refDagModal.value = measureDagModal;
			}

			measuresDagModel.main = props.measure.name;
			refDagModal.value.show();
		};

		const filteredEntry = function (measure) {
			const filteredCopy = Object.assign({}, measure);

			// Name is shown as header: no need to show it again in details
			delete filteredCopy.name;
			// Unclear why we receive a `key` which duplicate the `name`
			delete filteredCopy.key;
			// tags are shown as badges
			delete filteredCopy.tags;

			return filteredCopy;
		};

		return { mark, toggleInfo, filteredEntry };
	},
	template: /* HTML */ `
		<span v-html="mark(measure.name)" />
		<span type="button" :class="'badge text-bg-' + 'primary'" @click.prevent="toggleInfo()">
			<span>?</span>
		</span>
		<!-- Wizard-search waterfall score badge — mirror of the column variant. Rendered only
		     when an active search produced a sub-100 match (above-tier hits stay uncluttered). -->
		<span
			v-if="matchScore !== undefined && matchScore < 100"
			class="badge bg-light text-muted border ms-1"
			style="font-size: 0.68rem; font-weight: normal;"
			:title="'Wizard search match score (lower = looser match)'"
			>{{ matchScore }}%</span
		>
		<!-- Tag-bypass chip — same idiom as the column variant. Surfaced only on rows the
		     drop-tags fallback added: the user's tag filter would have excluded them, but no
		     row matched the tags so we relaxed them. -->
		<span
			v-if="matchTagsBypassed"
			class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle ms-1"
			style="font-size: 0.68rem; font-weight: normal;"
			title="This row does not match your tag filter — surfaced anyway because no row did. Drop or change your tags to refine."
			><i class="bi bi-tag-fill"></i> tag-bypass</span
		>
		<!-- Personal-history affinity chip. Mirrors the column variant — surfaced only when this
		     measure was used in past successful queries on this cube (historyScore > 0). -->
		<span
			v-if="historyScore > 0"
			class="badge bg-info-subtle text-info-emphasis border border-info-subtle ms-1"
			style="font-size: 0.68rem; font-weight: normal;"
			title="Used in past queries on this cube — boosted up the list. Cleared with browser data or by clearing this cube's history."
			><i class="bi bi-clock-history"></i
		></span>
		&nbsp;
		<AdhocQueryWizardMeasureTag v-for="tag in measure.tags" :tag="tag" :searchOptions="searchOptions" />
		<div v-if="showDetails" class="text-muted">
			<span v-if="measure.type == '.Aggregator' ">
				<small v-html="mark(measure.aggregationKey + '(' + measure.columnName + ')')" />
			</span>
			<span v-else-if="measure.type == 'eu.solven.adhoc.table.composite.SubMeasureAsAggregator'">
				<small v-html="mark(measure.aggregationKey + '(' + measure.subMeasure + ')')" />
			</span>
			<span v-else-if="measure.type == '.Combinator'">
				<small v-html="mark(measure.combinationKey + '(' + measure.underlyings.join(', ') + ')')" />
			</span>
			<span v-else-if="measure.type == '.Dispatchor'">
				<small v-html="'dispatching by ' + mark(measure.decompositionKey + '(' + measure.underlying + ')')" />
			</span>
			<small v-else-if="measure.type == '.Filtrator'">
				<ul>
					<li>underlying: <span v-html="mark(measure.underlying)"></span></li>
					<li>key: <span v-html="mark(measure.key)"></span></li>
					<li>tags: <span v-html="mark(measure.tags)"></span></li>
					<li>filter: <span v-html="mark(measure.filter)"></span></li>
				</ul>
			</small>
			<small v-else>
				<ul>
					<li v-for="(value, key) in filteredEntry(measure)">{{key}}: <span v-html="mark(value)"></span></li>
				</ul>
			</small>
		</div>
	`,
};
