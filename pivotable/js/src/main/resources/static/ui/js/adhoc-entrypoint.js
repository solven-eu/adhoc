import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";

import AdhocCubes from "./adhoc-cubes.js";

import AdhocEntrypointFormRef from "./adhoc-entrypoint-form-ref.js";

export default {
	components: {
		AdhocEntrypointHeader,
		AdhocCubes,
		AdhocEntrypointFormRef,
	},
	props: {
		entrypointId: {
			type: String,
			required: true,
		},
		showSchema: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbEntrypointFetching", "metadata"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId];
			},
		}),
		...mapState(useAdhocStore, {
			schema(store) {
				return store.schemas[this.entrypointId]?.schema;
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const nbCubes = ref("...");

		store.loadEntrypointIfMissing(props.entrypointId).then(() => {
			store.loadSchemas(props.entrypointId).then((schemas) => {
				if (schemas.length == 0) {
					nbCubes.value = 0;
				} else {
					nbCubes.value = Object.keys(schemas[0].schema.cubeToColumns).length;
				}
			});
		});

		// https://getbootstrap.com/docs/5.3/components/tooltips/
		// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		return { nbCubes };
	},
	template: /* HTML */ `
        <div v-if="!entrypoint && nbEntrypointFetching > 0">
            Loading
            <RouterLink :to="{path:'/html/servers/' + entrypointId}">entrypoint={{entrypointId}}</RouterLink>
        </div>
        <div v-else-if="entrypoint.error">{{entrypoint.error}}</div>
        <div v-else>
            <AdhocEntrypointHeader :entrypointId="entrypointId" />

            <span v-if="metadata.tags">
                Tags:
                <span class="badge text-bg-secondary" v-for="tag in entrypoint.tags" data-bs-toggle="tooltip" :data-bs-title="metadata.tags[tag]">{{tag}}</span
                ><br />
            </span>
            <span v-if="schema">
                <span v-if="showSchema">
                    Tables:
                    <ul v-for="(table, name) in schema.tableToColumns">
                        <li>
                            {{name}}
                            <ul v-for="(ref, name) in table.columnToTypes">
                                <li>{{name}}: {{ref}}</li>
                            </ul>
                        </li>
                    </ul>
                    Measures
                    <ul v-for="(measureBag, name) in schema.bagToMeasures">
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
                    <ul v-for="(cube, name) in schema.cubeToColumns">
                        <li>
                            {{name}}
                            <ul v-for="(ref, name) in cube.columnToTypes">
                                <li>{{name}}: {{ref}}</li>
                            </ul>
                        </li>
                    </ul>
                </span>
                <span v-else>
                    <div>
                        Tables:
                        <span v-for="(table, name) in schema.tableToColumns"> {{name}} </span>
                    </div>
                    <div>
                        Measures
                        <span v-for="(measureBag, name) in schema.bagToMeasures"> {{name}} </span>
                    </div>

                    <div>
                        Cubes
                        <span v-for="(cube, name) in schema.cubeToColumns">{{name}} </span>
                    </div>
                </span>
            </span>

            <!--span v-if="showContests">
                <AdhocCubes :entrypointId="entrypointId" :showserver="false" />
            </span>
            <span v-else>
                <RouterLink :to="{path:'/html/servers/' + entrypoint.entrypointId + '/contests'}"
                    ><i class="bi bi-trophy"></i> Join an existing contest ({{nbContests}})
                </RouterLink>
            </span-->

            <AdhocEntrypointFormRef :entrypointId="entrypointId" />
        </div>
    `,
};
