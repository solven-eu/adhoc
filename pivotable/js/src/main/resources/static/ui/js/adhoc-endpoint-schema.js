import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocEndpointSchemaRef from "./adhoc-endpoint-schema-ref.js";

import AdhocCube from "./adhoc-cube.js";
import AdhocCubeRef from "./adhoc-cube-ref.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
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
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const nbCubes = ref("...");
		
		const percentUi = ref(0);

		store.loadEndpointSchemaIfMissing(props.endpointId, (value, done, percent) => {percentUi.value = percent;}).then((schema) => {
			var endpointSchema = schema || { cubes: {} };
			nbCubes.value = Object.keys(endpointSchema.cubes).length;
		});

		// https://getbootstrap.com/docs/5.3/components/tooltips/
		// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
		// NOSONAR
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		return { nbCubes, percentUi };
	},
	template: /* HTML */ `
        <div v-if="!schema || schema.error">
            <AdhocLoading :id="endpointId" type="schema" :loading="nbSchemaFetching > 0" :error="schema.error" />
			
			{{percentUi}}
			<div class="progress" role="progressbar" aria-label="Animated striped example" :aria-valuenow="percentUi * 100" aria-valuemin="0" aria-valuemax="100">
			  <div class="progress-bar progress-bar-striped progress-bar-animated" :style="'width: ' + (percentUi * 100) + '%'"></div>
			</div>
        </div>
        <div v-else>
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
                    <span v-for="(table, name) in schema.tables"> {{name}} &nbsp;</span>
                </div>
                <div>
                    Measures
                    <span v-for="(measureBag, name) in schema.measureBags"> {{name}} &nbsp;</span>
                </div>

                <div>
                    Cubes
                    <span v-for="(cube, cubeName) in schema.cubes"> <AdhocCubeRef :endpointId="endpointId" :cubeId="cubeName" />&nbsp; </span>
                </div>
                <AdhocEndpointSchemaRef :endpointId="endpointId" />
            </span>
        </div>
    `,
};
