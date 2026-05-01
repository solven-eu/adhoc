import { ref, computed } from "vue";

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

		// Tag system. The user can pin free-form labels onto each favorite (e.g. "wip",
		// "demo", "owner=fraud-team") and filter the list by those labels. The filter
		// matches case-insensitively against ANY tag of an entry; an empty filter shows
		// all favorites. Tags are persisted alongside the queryModel via the preferences
		// store action `addTagToFavorite` / `removeTagFromFavorite`.
		const tagFilter = ref("");
		const newTagInputs = ref({}); // queryId -> in-flight string for the "add tag" input

		const filteredQueryModels = computed(() => {
			const needle = tagFilter.value.trim().toLowerCase();
			if (!needle) return queryModels;
			const result = {};
			for (const [id, entry] of Object.entries(queryModels)) {
				const tags = Array.isArray(entry.tags) ? entry.tags : [];
				if (tags.some((t) => t.toLowerCase().includes(needle))) {
					result[id] = entry;
				}
			}
			return result;
		});

		const addTag = function (queryId) {
			const value = (newTagInputs.value[queryId] || "").trim();
			if (!value) return;
			preferencesStore.addTagToFavorite(queryId, value);
			newTagInputs.value[queryId] = "";
		};

		const removeTag = function (queryId, tag) {
			preferencesStore.removeTagFromFavorite(queryId, tag);
		};

		// Export everything via a download-triggered <a>. Using a Blob + object URL keeps
		// the export off the clipboard so it survives large payloads and binary-safe edits
		// the user may want to do in a text editor. The payload is versioned (see
		// STORAGE_VERSION in store-preferences.js) so it round-trips losslessly through
		// `importFromFile` even after future schema bumps.
		const exportToFile = function () {
			const json = preferencesStore.exportFavorites();
			const blob = new Blob([json], { type: "application/json" });
			const url = URL.createObjectURL(blob);
			const a = document.createElement("a");
			a.href = url;
			a.download = "adhoc-favorites-" + new Date().toISOString().replace(/[:.]/g, "-") + ".json";
			document.body.appendChild(a);
			a.click();
			document.body.removeChild(a);
			URL.revokeObjectURL(url);
		};

		const importError = ref("");
		const importSummary = ref("");
		// File picker. Parses the file as text, delegates to the store action (which handles
		// versioning + migration + merging). Reports success or the thrown error message
		// directly in the modal so the user can fix the input and retry.
		// Import MERGES — existing favorites with the same id are overwritten, new ones appended.
		const importFromFile = function (event) {
			importError.value = "";
			importSummary.value = "";
			const file = event.target.files && event.target.files[0];
			if (!file) return;
			const reader = new FileReader();
			reader.onload = () => {
				try {
					const beforeCount = Object.keys(preferencesStore.queryModels).length;
					preferencesStore.importFavorites(reader.result);
					const afterCount = Object.keys(preferencesStore.queryModels).length;
					importSummary.value = "Imported " + (afterCount - beforeCount) + " new favorite(s).";
				} catch (e) {
					importError.value = e.message || String(e);
				} finally {
					// Reset the input so the same file can be re-imported (change fires even with same path).
					event.target.value = "";
				}
			};
			reader.onerror = () => {
				importError.value = "Failed to read file";
			};
			reader.readAsText(file);
		};

		return {
			preferencesStore,
			queryModels,
			filteredQueryModels,

			loadQuery,

			tagFilter,
			newTagInputs,
			addTag,
			removeTag,

			exportToFile,
			importFromFile,
			importError,
			importSummary,
		};
	},
	template: /* HTML */ `
		<!-- Button trigger modal -->
		<button type="button" class="btn btn-primary  btn-sm" data-bs-toggle="modal" data-bs-target="#queryFavorites">Favorites</button>

		<!--
			Teleport the modal to body. This component is rendered inside the floating Submit
			block (adhoc-query-executor.js), which uses transform: translate(...) for centering.
			That transform creates a new containing block for fixed-position descendants — the
			Bootstrap modal would otherwise be sized/positioned relative to the transformed
			ancestor, producing the symptom "the modal is only partially visible, the screen is
			greyed out and not operatable". Teleporting to body escapes the containing block.
			Same fix as adhoc-query-raw-modal.js / adhoc-query-favorite.js.
		-->
		<Teleport to="body">
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
							<!--
							Export / Import bar. Kept at the top of the favorites list so it's discoverable
							(users typically open this modal when they want to switch / browse favorites,
							so this is where they naturally look for bulk operations).
						-->
							<div class="d-flex gap-2 align-items-center flex-wrap border-bottom pb-2 mb-2">
								<button type="button" class="btn btn-outline-secondary btn-sm" @click="exportToFile">
									<i class="bi bi-download"></i> Export all favorites
								</button>
								<label class="btn btn-outline-secondary btn-sm mb-0">
									<i class="bi bi-upload"></i> Import favorites
									<input type="file" accept="application/json,.json" class="d-none" @change="importFromFile" />
								</label>
								<small v-if="importSummary" class="text-success">{{importSummary}}</small>
								<small v-if="importError" class="text-danger">Import failed: {{importError}}</small>
							</div>

							<!--
							Tag filter. Substring match (case-insensitive) against any tag of a
							favorite. Empty = show everything. Sits above the list so the user
							sees the input next to the list it filters.
						-->
							<div class="d-flex align-items-center gap-2 mb-2">
								<i class="bi bi-tags text-muted"></i>
								<input type="text" class="form-control form-control-sm" placeholder="Filter favorites by tag…" v-model="tagFilter" />
							</div>

							<ul v-for="(queryModel,queryId) in filteredQueryModels" class="list-group">
								<li class="list-group-item">
									<div class="adhoc-favorite-load" @click="loadQuery(queryId)" style="cursor: pointer">
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
									</div>
									<!--
									Tag editor. @click.stop on the row container above stops the
									tag-pill / input clicks from bubbling up and accidentally
									firing loadQuery — that would be very surprising UX.
								-->
									<div class="d-flex align-items-center flex-wrap gap-1 mt-2" @click.stop>
										<span v-for="t in (queryModel.tags || [])" :key="t" class="badge rounded-pill text-bg-info">
											{{t}}
											<button
												type="button"
												class="btn-close btn-close-white ms-1"
												style="font-size: 0.5rem; vertical-align: middle"
												aria-label="Remove tag"
												@click="removeTag(queryId, t)"
											></button>
										</span>
										<input
											type="text"
											class="form-control form-control-sm"
											style="max-width: 12rem"
											placeholder="+ tag"
											v-model="newTagInputs[queryId]"
											@keydown.enter.prevent="addTag(queryId)"
										/>
										<button
											type="button"
											class="btn btn-outline-secondary btn-sm"
											@click="addTag(queryId)"
											:disabled="!newTagInputs[queryId]"
										>
											<i class="bi bi-plus"></i>
										</button>
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
		</Teleport>
	`,
};
