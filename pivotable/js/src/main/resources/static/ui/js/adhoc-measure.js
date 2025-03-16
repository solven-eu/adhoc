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
        <span v-if="measure.type == '.Aggregator'"> {{measure.name}}: {{measure.aggregationKey}}({{measure.columnName}}) </span>
        <span v-else-if="measure.type == '.Combinator'"> {{measure.name}}: {{measure.combinationKey}}({{measure.underlyings.join(', ')}}) </span>
        <span v-else> {{measure.name}}: {{measure}} </span>
    `,
};
