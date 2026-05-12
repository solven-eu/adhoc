// @ts-check
import { ref, computed } from "vue";

import { mapState } from "pinia";
import { useUserStore } from "./store-user.js";

// MCP-token helper: copies the current access_token to the clipboard so it can be pasted into an
// MCP client's `.mcp.json` as `"headers": { "Authorization": "Bearer …" }`. The token is the same
// JWT the SPA already uses for `/api/**` calls — re-using it means MCP requests authenticate via
// the existing `PivotableJwtWebfluxSecurity` chain with no extra server-side wiring. The button
// shows a transient "Copied!" status; users who can't use the clipboard API (insecure context,
// permission denied) can read the value from the disclosed `<details>` block.

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
// import { Tooltip } from "bootstrap";

import AdhocAccountChip from "./adhoc-account-chip.js";

import Flag from "./flag.js";

export default {
	components: {
		AdhocAccountChip,
		Flag,
	},
	computed: {
		...mapState(useUserStore, ["nbLoginLoading", "account", "isLoggedIn"]),
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
				/** @type {Record<string, string>} */
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

		// MCP token affordance: surface the current access_token so an MCP client (e.g. Claude Code
		// via .mcp.json) can carry it as a Bearer header on requests to /mcp/**. Empty when the SPA
		// has not yet acquired (or has already lost) a token.
		const mcpToken = computed(() => userStore.tokens.access_token || "");

		// Sanity-check curl: pre-baked with the live origin and the current Bearer token so the user
		// can copy + paste into a terminal to verify the SSE endpoint accepts the token. `-N` disables
		// curl's output buffering so the SSE `event: endpoint` line shows up immediately rather than
		// being held back behind libcurl's default 1KB chunk threshold.
		const mcpCurl = computed(() => {
			if (!mcpToken.value) {
				return "";
			}
			return `curl -N -H "Authorization: Bearer ${mcpToken.value}" ${window.location.origin}/mcp/sse`;
		});

		// Two independent "copied" flashes so clicking one button does not reset the other's state.
		const mcpCopied = ref(false);
		const mcpCurlCopied = ref(false);

		// Shared clipboard helper — wraps navigator.clipboard with the textarea fallback used elsewhere
		// (adhoc-query-grid-clipboard.js) so insecure contexts (HTTP without TLS) still work. `flash`
		// is the ref that flips to true for ~2s on success so the button shows "Copied!".
		const copyToClipboard = function (value, flash, timerHolder) {
			if (!value) {
				return;
			}
			const onCopied = function () {
				flash.value = true;
				if (timerHolder.id) {
					clearTimeout(timerHolder.id);
				}
				timerHolder.id = setTimeout(() => {
					flash.value = false;
				}, 2_000);
			};
			if (navigator.clipboard && navigator.clipboard.writeText) {
				navigator.clipboard
					.writeText(value)
					.then(onCopied)
					.catch((e) => {
						console.warn("Clipboard write failed", e);
					});
				return;
			}
			const ta = document.createElement("textarea");
			ta.value = value;
			ta.style.position = "fixed";
			ta.style.opacity = "0";
			document.body.appendChild(ta);
			ta.select();
			try {
				document.execCommand("copy");
				onCopied();
			} catch (e) {
				console.warn("execCommand copy failed", e);
			}
			document.body.removeChild(ta);
		};

		// Holders are objects so setTimeout's return id survives across renders without re-creating refs.
		const mcpCopyTimer = { id: null };
		const mcpCurlCopyTimer = { id: null };
		const copyMcpToken = () => copyToClipboard(mcpToken.value, mcpCopied, mcpCopyTimer);
		const copyMcpCurl = () => copyToClipboard(mcpCurl.value, mcpCurlCopied, mcpCurlCopyTimer);

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

		return {
			countryCode,
			countries,
			updateCountry,
			mcpToken,
			mcpCurl,
			mcpCopied,
			mcpCurlCopied,
			copyMcpToken,
			copyMcpCurl,
		};
	},
	template: /* HTML */ `
		<span>
			<AdhocAccountChip :accountId="account.accountId" /><br />
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

			<div class="mt-2" v-if="mcpToken">
				<button
					type="button"
					class="btn btn-sm btn-outline-secondary"
					data-testid="copy-mcp-token"
					@click="copyMcpToken()"
					title="Copy the current Bearer token, ready to paste into an MCP client's .mcp.json under headers.Authorization"
				>
					<i class="bi bi-clipboard"></i>
					<span v-if="!mcpCopied"> Copy MCP token</span>
					<span v-else> Copied!</span>
				</button>
				<button
					type="button"
					class="btn btn-sm btn-outline-secondary ms-1"
					data-testid="copy-mcp-curl"
					@click="copyMcpCurl()"
					title="Copy a ready-to-run curl command (Bearer header injected) that probes /mcp/sse — useful to verify the token works before restarting your MCP client"
				>
					<i class="bi bi-terminal"></i>
					<span v-if="!mcpCurlCopied"> Copy MCP curl</span>
					<span v-else> Copied!</span>
				</button>
				<details class="mt-1">
					<summary class="small text-muted">Show MCP token (paste into .mcp.json)</summary>
					<pre class="small mb-0" style="white-space: pre-wrap; word-break: break-all">Authorization: Bearer {{mcpToken}}</pre>
				</details>
				<details class="mt-1">
					<summary class="small text-muted">Show MCP curl (run in terminal to verify)</summary>
					<pre class="small mb-0" style="white-space: pre-wrap; word-break: break-all">{{mcpCurl}}</pre>
				</details>
			</div>
		</span>
	`,
};
