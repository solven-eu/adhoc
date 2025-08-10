import { ref, watch } from "vue";

import { useRouter } from "vue-router";

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

		const queryModelHash = preferencesStore.hashQuery(props.queryModel);

		const queryModels = preferencesStore.queryModels;

		const queryName = ref("");

		const remove = function () {
			// Remove the queryModel (not to clutter the list of models)
			delete preferencesStore.queryModels.currentQueryId;
			// Switch to draft-mode
			preferencesStore.currentQueryId = undefined;
		};
		const saveChanges = function () {
			preferencesStore.saveQuery(props.queryModel, queryName.value);
		};
		const saveNew = function () {
			const queryId = preferencesStore.saveQuery(props.queryModel, queryName.value);
			preferencesStore.registerLatestQueryId(queryId);
		};

		{
			const router = useRouter();

			preferencesStore.latestQueryIds = JSON.parse(localStorage.getItem("adhoc.preferences.latestQueryIds")) || [];
			if (preferencesStore.latestQueryIds >= 1) {
				const latestQueryId = preferencesStore.latestQueryIds[preferencesStore.latestQueryIds.length - 1];
				// TODO Restore latestQueryId
			}

			preferencesStore.queryModels = JSON.parse(localStorage.getItem("adhoc.preferences.queryModels")) || {};

			watch(
				preferencesStore.latestQueryIds,
				(latestQueryIds) => {
					// persist the whole state to the local storage whenever it changes
					localStorage.setItem("adhoc.preferences.latestQueryIds", JSON.stringify(latestQueryIds));
				},
				{ deep: true },
			);
			watch(
				preferencesStore.queryModels,
				(queryModels) => {
					// persist the whole state to the local storage whenever it changes
					localStorage.setItem("adhoc.preferences.queryModels", JSON.stringify(queryModels));
				},
				{ deep: true },
			);
		}

		const editNameFlag = ref(false);

		return {
			preferencesStore,
			queryModelHash,
			queryModels,
			queryName,

			remove,
			saveChanges,
			saveNew,

			editNameFlag,
		};
	},
	template: /* HTML */ `
        <!-- Button trigger modal -->
        <button type="button" class="btn btn-primary  btn-sm" data-bs-toggle="modal" data-bs-target="#queryFavorite">
            Favorite

            <span v-if="preferencesStore.hasUnsavedChanges(queryModel)"> * </span>
        </button>

        <!-- Modal -->
        <div class="modal fade" id="queryFavorite" tabindex="-1" aria-labelledby="exampleModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="exampleModalLabel">Save query as Favorite</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <!-- https://stackoverflow.com/questions/4611591/code-vs-pre-vs-samp-for-inline-and-block-code-snippets -->
                    <div class="modal-body">
                        <div>
                            <div>columns: {{queryModel.selectedColumnsOrdered}}</div>
                            <div>measures: {{queryModel.measures()}}</div>
                            <div>filter: {{queryModel.filter}}</div>
                            <div>options: {{queryModel.options()}}</div>
                            <div>customMarker: {{queryModel.customMarker()}}</div>
                        </div>

                        <div v-if="preferencesStore.currentQueryId">
                            <div>id = {{preferencesStore.currentQueryId}}</div>
                            <div>name = 
								<span v-if="!editNameFlag" @click="editNameFlag = true" > {{queryModels[preferencesStore.currentQueryId].name}}</span>
								<span v-else>
									<input type="text" v-model="queryModels[preferencesStore.currentQueryId].name"></input>
								 </span>
							</div>
                            <div>
                                <button type="button" class="btn btn-primary" @click="remove">Remove</button>

                                <span v-if="preferencesStore.hasUnsavedChanges(queryModel)">
                                    <button type="button" class="btn btn-primary" @click="saveChanges">Save changes</button>
                                </span>
                            </div>
						</div>
                        <div v-else>
                            <input type="text" class="form-control" placeholder="Query name" aria-label="Query name" v-model="queryName" />
                            <button type="button" class="btn btn-primary" @click="saveNew">Save</button>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `,
};
