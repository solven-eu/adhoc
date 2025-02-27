export default {
	props: {
		entrypointId: {
			type: String,
			required: true,
		},
	},
	setup() {
		return {};
	},
	template: /* HTML */ ` <RouterLink :to="{path:'/html/entrypoints/' + entrypointId + '/schema'}"><i class="bi bi-node-plus"></i> Show schema</RouterLink> `,
};
