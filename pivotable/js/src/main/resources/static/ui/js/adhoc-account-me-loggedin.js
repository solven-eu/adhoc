import { ref, computed } from "vue";

import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
// import { Tooltip } from "bootstrap";

import AdhocAccountRef from "./adhoc-account-ref.js";

import Flag from "./flag.js";

export default {
	components: {
		AdhocAccountRef,
		Flag,
	},
	computed: {
		...mapState(useUserStore, ["nbAccountFetching", "account", "isLoggedIn"]),
	},
	setup() {
		const userStore = useUserStore();

		const countries = ref({});
		// https://flagpedia.net/download/api
		fetch("https://flagcdn.com/en/codes.json")
			.then((response) => {
				if (!response.ok) {
					console.warn("Issue downloading countries");
				}
				return response.json();
			})
			.then((json) => {
				countries.value = json;
			});

		const countryCode = computed(() => userStore.account.details.countryCode || "unknown");

		const updateCountry = function (newCountryCode) {
			console.log("Update accouht country", newCountryCode);

			// Update the store asap
			userStore.account.details.countryCode = newCountryCode;

			const userUpdates = {};
			userUpdates.countryCode = newCountryCode;

			userStore.fetchCsrfToken().then((csrfToken) => {
				const headers = {};
				headers[csrfToken.header] = csrfToken.token;
				headers["Content-Type"] = "application/json";

				const fetchOptions = {
					method: "POST",
					headers: headers,
					body: JSON.stringify(userUpdates),
				};
				fetch("/api/login/v1/user", fetchOptions)
					.then((response) => {
						if (!response.ok) {
							throw userStore.newNetworkError("POST for userUpdate has failed ", "/api/login/v1/user", response);
						}

						return response.json();
					})
					.then((updatedUser) => {
						// The submitted move may have impacted the user
						userStore.$patch((state) => {
							state.account = updatedUser;
						});
					})
					.catch((e) => {
						userStore.onSwallowedError(e);
					});
			});
		};

		if (countryCode.value === "unknown") {
			console.log("The account has no countryCode");
			// https://www.techighness.com/post/get-user-country-and-region-on-browser-with-javascript-only/
			fetch("https://unpkg.com/moment-timezone/data/meta/latest.json")
				.then((response) => {
					if (!response.ok) {
						console.warn("Issue downloading timezone info");
					}
					return response.json();
				})
				.then((json) => {
					console.debug("timezones", json);

					const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
					const countryCodes = json.zones[userTimezone].countries;

					console.log("countryCodes", countryCodes);

					if (countryCodes.length === 0) {
						console.warn("No country for timezone", userTimezone);
						return;
					}

					if (countryCode.value === "unknown") {
						updateCountry(countryCodes[0]);
					}
				});
		}

		return { countryCode, countries, updateCountry };
	},
	template: /* HTML */ `
        <span>
            <AdhocAccountRef :accountId="account.accountId" /><br />
            <span v-if="account.details">
                username={{account.details.username}}<br />
                name={{account.details.name}}<br />
                email={{account.details.email}}<br />
            </span>

            <div>
                <div class="col my-auto">
                    <span class="btn-group ">
                        <button type="button" class="btn btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown" aria-expanded="false">
                            Current country: {{countries[countryCode] || countryCode}}
                            <Flag :country="countryCode" />
                        </button>
                        <ul class="dropdown-menu">
                            <li>
                                <a
                                    class="dropdown-item"
                                    @click="updateCountry(countryCode)"
                                    :data-testid="'country_' + countryIndex"
                                    v-for="(countryName, countryCode, countryIndex) in countries"
                                >
                                    <Flag :country="countryCode" />{{countryName}}
                                </a>
                            </li>
                        </ul>
                    </span>
                </div>
            </div>
        </span>
    `,
};
