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
		<div v-if="isLoading" class="d-flex align-items-center gap-2">
			<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
			<span>Loading login options…</span>
		</div>
		<div v-else-if="modal && selectedProvider == 'BASIC'">
			<LoginBasic :modal="modal" />
		</div>
		<!--
			Provider chooser. Replaces an earlier <li>-based layout that rendered the bare
			bullet markers and was missing an icon for the BASIC option (it used to be a
			plain text link). Each provider is now a Bootstrap list-group action item; BASIC
			uses bi-person-fill-lock as its icon, OAuth providers use the backend-supplied
			button_img if any.
		-->
		<div v-else class="list-group">
			<template v-for="item in loginProviders" :key="item.registration_id">
				<a
					v-if="item.login_url &amp;&amp; modal &amp;&amp; item.type == 'basic'"
					href="#"
					class="list-group-item list-group-item-action d-flex align-items-center gap-2"
					data-testid="login-basic-open"
					@click.prevent="selectedProvider = 'BASIC'"
				>
					<i class="bi bi-person-fill-lock fs-4 text-secondary"></i>
					<span>{{ item.registration_id }}</span>
				</a>
				<a v-else-if="item.login_url" :href="item.login_url" class="list-group-item list-group-item-action d-flex align-items-center gap-2">
					<img
						v-if="item.button_img"
						:src="item.button_img"
						:alt="item.registration_id"
						style="max-height:32px;max-width:120px;height:auto;width:auto;"
					/>
					<i v-else-if="item.type == 'basic'" class="bi bi-person-fill-lock fs-4 text-secondary"></i>
					<i v-else class="bi bi-box-arrow-in-right fs-4 text-secondary"></i>
					<span>{{ item.registration_id }}</span>
				</a>
				<span v-else class="list-group-item text-muted small">{{ item.registration_id }}</span>
			</template>
		</div>
	`,
};
