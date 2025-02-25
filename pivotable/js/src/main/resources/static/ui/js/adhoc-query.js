import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

import AdhocEntrypointHeader from "./adhoc-entrypoint-header.js";
import AdhocCubeHeader from "./adhoc-cube-header.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {
		AdhocEntrypointHeader,
		AdhocCubeHeader,
	},
	// https://vuejs.org/guide/components/props.html
	props: {
		cubeId: {
			type: String,
			required: true,
		},
		entrypointId: {
			type: String,
			required: true,
		},
		showEntrypoint: {
			type: Boolean,
			default: true,
		},
		showLeaderboard: {
			type: Boolean,
			default: true,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching"]),
		...mapState(useAdhocStore, {
			entrypoint(store) {
				return store.entrypoints[this.entrypointId] || { error: "not_loaded" };
			},
			schema(store) {
				return store.schemas[this.entrypointId] || { error: "not_loaded" };
			},
			cube(store) {
				return store.schemas[this.entrypointId]?.cubes[this.cubeId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		store.loadCubeSchemaIfMissing(props.cubeId, props.entrypointId);

		return {};
	},
	template: /* HTML */ `
        <div v-if="(!entrypoint || !cube)">
            <div v-if="(nbSchemaFetching > 0 || nbContestFetching > 0)">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading cubeId={{cubeId}}</span>
                </div>
            </div>
            <div v-else>
                <span>Issue loading cubeId={{cubeId}}</span>
            </div>
        </div>
        <div v-else-if="entrypoint.error || cube.error">{{entrypoint.error || cube.error}}</div>
        <div v-else>
            <AdhocCubeHeader :entrypointId="entrypointId" :cubeId="cubeId" />
            <AdhocEntrypointHeader :entrypointId="entrypointId" :withDescription="false" v-if="showEntrypoint" />

			Build the query

			Columns 
			<ul v-for="(type, name) in cube.columns.columnToTypes">
			    <li>
					<div class="form-check form-switch">
					  <input class="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDefault">
					  <label class="form-check-label" for="flexSwitchCheckDefault">{{name}}: {{type}}</label>
					</div>
				</li>
			</ul>

			Measures 
			<ul v-for="ref in Object.values(cube.measures)">
			    <li>
					<div class="form-check form-switch">
					  <input class="form-check-input" type="checkbox" role="switch" id="flexSwitchCheckDefault">
					  <label class="form-check-label" for="flexSwitchCheckDefault">
						  <span v-if="ref.type == '.Aggregator'"> {{ref.name}}: {{ref.aggregationKey}}({{ref.columnName}}) </span>
						  <span v-else-if="ref.type == '.Combinator'"> {{ref.name}}: {{ref.combinationKey}}({{ref.underlyings.join(', ')}}) </span>
						  <span v-else> {{ref.name}}: {{ref}} </span>
					  </label>
					</div>
			    </li>
			</ul>
        </div>
    `,
};
