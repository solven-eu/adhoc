export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		const resetQuery = function () {
			props.queryModel.reset();
		};

		return {
			resetQuery,
		};
	},
	template: /* HTML */ ` <button type="button" class="btn btn-outline-warning  btn-sm" @click="resetQuery">Reset query</button> `,
};
