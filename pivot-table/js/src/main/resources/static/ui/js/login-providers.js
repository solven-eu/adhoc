import { ref } from "vue";

export default {
	setup() {
		const error = ref({});
		const isLoading = ref(true);
		const loginProviders = ref({
			loginProviders: [],
		});

		async function fetchTheUrl(url) {
			try {
				isLoading.value = true;
				const response = await fetch(url);
				const responseJson = await response.json();
				loginProviders.value = responseJson.list;
			} catch (e) {
				console.error("Issue on Network: ", e);
				error.value = e;
			} finally {
				isLoading.value = false;
			}
		}

		fetchTheUrl("/api/login/v1/providers");

		return { isLoading, loginProviders };
	},
	template: /* HTML */ `
        <div v-if="isLoading">Loading login options</div>
        <div v-else>
            <li v-for="item in loginProviders">
                {{}}
                <a :href="item.login_url">
                    <img
                        v-if="item.button_img"
                        :src="item.button_img"
                        :alt="item.registration_id"
                        style="max-height:50px;max-width:200px;height:auto;width:auto;"
                    />
                    <span v-else>{{ item.registration_id }}</span>
                </a>
            </li>
        </div>
    `,
};
