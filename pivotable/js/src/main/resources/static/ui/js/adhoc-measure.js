import {} from "vue";

export default {
	props: {
		measure: {
			type: Object,
			required: true,
		},
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
        <span v-if="measure.type == '.Aggregator'"> {{measure.name}} <small class="text-muted">{{measure.aggregationKey}}({{measure.columnName}})</small></span>
        <span v-else-if="measure.type == '.Combinator'">
            {{measure.name}} <small class="text-muted">{{measure.combinationKey}} ({{measure.underlyings.join(', ')}})</small>
        </span>
        <span v-else-if="measure.type == '.Dispatchor'">
            {{measure.name}} <small class="text-muted">dispatching by {{measure.decompositionKey}} ({{measure.underlyings.join(', ')}})</small>
        </span>
        <span v-else-if="measure.type == '.Filtrator'">
            {{measure.name}} <small class="text-muted">filtering {{measure.filter}} ({{measure.underlying}})</small>
        </span>
        <span v-else> {{measure.name}} <small class="text-muted">{{measure}}</small> </span>
    `,
};
