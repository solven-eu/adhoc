import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocEndpointHeader from "./adhoc-endpoint-header.js";

import AdhocCubes from "./adhoc-cubes.js";

import AdhocEndpointSchemaRef from "./adhoc-endpoint-schema-ref.js";

import AdhocCube from "./adhoc-cube.js";
import AdhocCubeRef from "./adhoc-cube-ref.js";

export default {
	components: {
		AdhocEndpointHeader,
		AdhocCubes,
		AdhocEndpointSchemaRef,
		AdhocCube,
		AdhocCubeRef,
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
		}),
		...mapState(useAdhocStore, {
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
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		return { nbCubes };
	},
	template: /* HTML */ `
        <div v-if="!endpoint && nbSchemaFetching > 0">
            Loading
            <RouterLink :to="{path:'/html/endpoints/' + endpointId}">endpoint={{endpointId}}</RouterLink>
        </div>
        <div v-else-if="endpoint.error">{{endpoint.error}}</div>
        <div v-else>
            <AdhocEndpointHeader :endpointId="endpointId" />

            <span v-if="metadata.tags">
                Tags:
                <span class="badge text-bg-secondary" v-for="tag in endpoint.tags" data-bs-toggle="tooltip" :data-bs-title="metadata.tags[tag]">{{tag}}</span
                ><br />
            </span>
            <span v-if="schema">
                <span v-if="showSchema">
                    Tables:
                    <ul v-for="(table, name) in schema.tables">
                        <li>
                            {{name}}
                            <ul v-for="(ref, name) in table.columnToTypes">
                                <li>{{name}}: {{ref}}</li>
                            </ul>
                        </li>
                    </ul>
                    Measures
                    <ul v-for="(measureBag, name) in schema.measureBags">
                        <li>
                            {{name}}
                            <ul v-for="ref in measureBag">
                                <li>
                                    <span v-if="ref.type == '.Aggregator'"> {{ref.name}}: {{ref.aggregationKey}}({{ref.columnName}}) </span>
                                    <span v-else-if="ref.type == '.Combinator'"> {{ref.name}}: {{ref.combinationKey}}({{ref.underlyings.join(', ')}}) </span>
                                    <span v-else> {{ref.name}}: {{ref}} </span>
                                </li>
                            </ul>
                        </li>
                    </ul>

                    Cubes
                    <ul v-for="(cube, cubeName) in schema.cubes">
                        <li>
							<AdhocCubeRef :endpointId="endpointId" :cubeId="cubeName" />
                            <ul v-for="(ref, name) in cube.columns.columnToTypes">
                                <li>{{name}}: {{ref}}</li>
                            </ul>
                        </li>
                    </ul>
                </span>
                <span v-else>
                    <div>
                        Tables:
                        <span v-for="(table, name) in schema.tables"> {{name}} </span>
                    </div>
                    <div>
                        Measures
                        <span v-for="(measureBag, name) in schema.measureBags"> {{name}} </span>
                    </div>

                    <div>
                        Cubes
                        <span v-for="(cube, cubeName) in schema.cubes">
							<AdhocCubeRef :endpointId="endpointId" :cubeId="cubeName" />
						</span>
                    </div>
					<AdhocEndpointSchemaRef :endpointId="endpointId" />
                </span>
            </span>

            <!--span v-if="showContests">
                <AdhocCubes :endpointId="endpointId" :showserver="false" />
            </span>
            <span v-else>
                <RouterLink :to="{path:'/html/servers/' + endpoint.endpointId + '/contests'}"
                    ><i class="bi bi-trophy"></i> Join an existing contest ({{nbContests}})
                </RouterLink>
            </span-->
			
			<AdhocCube :endpointId="endpointId" :cubeId="cubeId" v-if="cubeId" />
        </div>
    `,
};
