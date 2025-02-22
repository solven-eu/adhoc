export default {
	props: {
		country: {
			type: String,
			required: true,
		},
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
        <img v-if="country != 'unknown'" :src="'https://flagcdn.com/' + country.toLowerCase() + '.svg'" :alt="country" width="36" height="24" />
    `,
};
