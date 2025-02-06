import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocserverHeader from "./adhoc-server-header.js";

import AdhocCubes from "./adhoc-cubes.js";

import AdhocServerFormRef from "./adhoc-server-form-ref.js";

export default {
	components: {
		AdhocserverHeader,
		AdhocCubes,
		AdhocServerFormRef,
	},
	props: {
		serverId: {
			type: String,
			required: true,
		},
		showContests: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbServerFetching", "metadata"]),
		...mapState(useAdhocStore, {
			server(store) {
				return store.servers[this.serverId];
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const nbContests = ref("...");

		store.loadserverIfMissing(props.serverId).then(() => {
			store.loadContests(props.serverId).then((contests) => {
				nbContests.value = contests.length;
			});
		});

		// https://getbootstrap.com/docs/5.3/components/tooltips/
		// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		return { nbContests };
	},
	template: /* HTML */ `
        <div v-if="!server && nbServerFetching > 0">Loading <RouterLink :to="{path:'/html/servers/' + serverId}">server={{serverId}}</RouterLink></div>
        <div v-else-if="server.error">{{server.error}}</div>
        <div v-else>
            <AdhocserverHeader :serverId="serverId" />

            <span v-if="metadata.tags">
                Tags: <span class="badge text-bg-secondary" v-for="tag in server.tags" data-bs-toggle="tooltip" :data-bs-title="metadata.tags[tag]">{{tag}}</span
                ><br />
            </span>
            <ul v-for="ref in server.references">
                <li><a :href="ref" target="_blank">{{ref}}</a></li>
            </ul>

            <span v-if="showContests">
                <AdhocCubes :serverId="serverId" :showserver="false" />
            </span>
            <span v-else>
                <RouterLink :to="{path:'/html/servers/' + server.serverId + '/contests'}"
                    ><i class="bi bi-trophy"></i> Join an existing contest ({{nbContests}})
                </RouterLink>
            </span>

            <AdhocServerFormRef :serverId="serverId" />
        </div>
    `,
};
