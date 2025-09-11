import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";

import AdhocCubes from "./adhoc-cubes.js";

import AdhocEndpointSchemaRef from "./adhoc-endpoint-schema-ref.js";
import AdhocEndpointSchema from "./adhoc-endpoint-schema.js";

import AdhocCube from "./adhoc-cube.js";
import AdhocCubeRef from "./adhoc-cube-ref.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
		AdhocEndpointHeader,
		AdhocCubes,
		AdhocEndpointSchema,
		AdhocEndpointSchemaRef,
		AdhocCube,
		AdhocCubeRef,
		AdhocLoading,
	},
	props: {
		endpointId: {
			type: String,
			required: true,
		},
		cubeId: {
			type: String,
			required: false,
		},
		showSchema: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching", "metadata"]),
		...mapState(useAdhocStore, {
			endpoint(store) {
				return store.endpoints[this.endpointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const nbCubes = ref("...");

		store.loadEndpointSchemaIfMissing(props.endpointId).then((schema) => {
			var endpointSchema = schema || { cubes: {} };
			nbCubes.value = Object.keys(endpointSchema.cubes).length;
		});

		// https://getbootstrap.com/docs/5.3/components/tooltips/
		// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
		// NOSONAR
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		return { nbCubes };
	},
	template: /* HTML */ `
		<div v-if="!endpoint || endpoint.error">
		    <AdhocLoading :id="endpointId" type="endpoint" :loading="nbSchemaFetching > 0" :error="endpoint.error" />
		</div>
        <div v-else>
            <AdhocEndpointHeader :endpointId="endpointId" />

            <span v-if="metadata.tags">
                Tags:
                <span class="badge text-bg-secondary" v-for="tag in endpoint.tags" data-bs-toggle="tooltip" :data-bs-title="metadata.tags[tag]">{{tag}}</span>
            </span>
            <span v-if="schema">
                <AdhocEndpointSchema :endpointId="endpointId" :cubeId="cubeId" :showSchema="showSchema" />
            </span>

            <AdhocCube :endpointId="endpointId" :cubeId="cubeId" v-if="cubeId" />
        </div>
    `,
};
