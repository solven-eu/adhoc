import { ref } from "vue";

import LoginBasic from "./login-basic.js";

export default {
	components: {
		LoginBasic,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		modal: {
			type: Boolean,
			default: false,
		},
	},
	setup() {
		const error = ref({});
		const isLoading = ref(true);
		const loginProviders = ref({
			loginProviders: [],
		});

		const selectedProvider = ref(null);

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

		return { isLoading, loginProviders, selectedProvider };
	},
	template: /* HTML */ `
        <div v-if="isLoading">Loading login options</div>
        <div v-else-if="modal && selectedProvider == 'BASIC'">
            <LoginBasic :modal="modal" />
        </div>
        <div v-else>
            <li v-for="item in loginProviders">
                <span v-if="item.login_url">
                    <span v-if="modal">
                        <!-- Login in the modal, while the route is some other component -->
                        <!-- https://stackoverflow.com/questions/51223214/vue-js-how-to-prevent-browser-from-going-to-href-link-and-instead-only-execute -->
                        <span v-if="item.registration_id == 'BASIC'">
                            <a href="#" @click.prevent="selectedProvider = item.registration_id"> {{ item.registration_id }} </a>
                        </span>
                        <span v-else>
                            <a :href="item.login_url">
                                <img
                                    v-if="item.button_img"
                                    :src="item.button_img"
                                    :alt="item.registration_id"
                                    style="max-height:50px;max-width:200px;height:auto;width:auto;"
                                />
                                <span v-else>{{ item.registration_id }}</span>
                            </a>
                        </span>
                    </span>
                    <span v-else>
                        <!-- On the main login route: we can do path updates -->
                        <a :href="item.login_url">
                            <img
                                v-if="item.button_img"
                                :src="item.button_img"
                                :alt="item.registration_id"
                                style="max-height:50px;max-width:200px;height:auto;width:auto;"
                            />
                            <span v-else>{{ item.registration_id }}</span>
                        </a>
                    </span>
                </span>
                <span v-else>
                    <!-- Happens on notAvailable providers (like 'loading'): just show the text -->
                    {{ item.registration_id }}
                </span>
            </li>
        </div>
    `,
};
