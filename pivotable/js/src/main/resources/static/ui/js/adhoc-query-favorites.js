import { ref, watch } from "vue";

import { usePreferencesStore } from "./store-preferences.js";

export default {
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const preferencesStore = usePreferencesStore();

		const queryModels = preferencesStore.queryModels;

		const loadQuery = function (queryId) {
			preferencesStore.loadQuery(queryId, props.queryModel, true);
		};

		return {
			preferencesStore,
			queryModels,

			loadQuery,
		};
	},
	template: /* HTML */ `
        <!-- Button trigger modal -->
        <button type="button" class="btn btn-primary  btn-sm" data-bs-toggle="modal" data-bs-target="#queryFavorites">Favorites</button>

        <!-- Modal -->
        <div class="modal fade" id="queryFavorites" tabindex="-1" aria-labelledby="exampleModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="exampleModalLabel">Load a Favorite</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <!-- https://stackoverflow.com/questions/4611591/code-vs-pre-vs-samp-for-inline-and-block-code-snippets -->
                    <div class="modal-body">
                        <ul v-for="(queryModel,queryId) in queryModels" class="list-group">
                            <li class="list-group-item " @click="loadQuery(queryId)">
                                <small>id={{queryId}}</small>
                                <div>name={{queryModel.name}}</div>
                                <div>path={{queryModel.path}}</div>
                                <div>
                                    <div>columns: {{queryModel.queryModel.columns}}</div>
                                    <div>measures: {{queryModel.queryModel.measures}}</div>
                                    <div>filter: {{queryModel.queryModel.filter}}</div>
                                    <div>options: {{queryModel.queryModel.options}}</div>
                                    <div>customMarker: {{queryModel.queryModel.customMarker}}</div>
                                </div>
                            </li>
                        </ul>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `,
};
