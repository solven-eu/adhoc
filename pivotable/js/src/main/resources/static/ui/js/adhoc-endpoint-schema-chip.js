export default {
	props: {
		endpointId: {
			type: String,
			required: true,
		},
	},
	setup() {
		return {};
	},
	template: /* HTML */ ` <RouterLink :to="{path:'/html/endpoints/' + endpointId + '/schema'}"><i class="bi bi-node-plus"></i> Show schema</RouterLink> `,
};
