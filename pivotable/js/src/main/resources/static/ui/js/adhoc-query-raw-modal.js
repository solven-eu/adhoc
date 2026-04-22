import { ref, computed, watch } from "vue";

import { queryModelToMdx } from "./adhoc-query-to-mdx.js";

export default {
	props: {
		queryJson: {
			type: Object,
			required: true,
		},
		queryModel: {
			type: Object,
			required: true,
		},
		// Cube name used as the FROM target when rendering the query as MDX. Optional; if
		// omitted the MDX tab still renders with an empty cube name (`FROM []`), which is
		// still useful for copy-paste.
		cubeId: {
			type: String,
			required: false,
			default: "",
		},
	},
	setup(props) {
		// Which read-only view to show in the modal body. "json" preserves the original
		// behaviour (with edit mode); "mdx" shows a derived MDX string for side-by-side
		// comparison with other MDX clients. MDX is always read-only — we don't convert
		// back from MDX to queryModel.
		const activeTab = ref("json");

		// By default, we're not editing but just looking at the JSON
		const isEditing = ref(false);
		const editedJson = ref(JSON.stringify(props.queryJson, null, 2));

		const copyToClipboardStatus = ref("");

		watch(
			() => props.queryJson,
			(newQueryJson) => {
				editedJson.value = JSON.stringify(props.queryJson, null, 2);
				copyToClipboardStatus.value = "";
			},
		);

		// Live MDX projection of the current queryModel. Recomputes reactively when the
		// model changes, so the modal can stay open while the user edits the wizard.
		const mdxString = computed(() => queryModelToMdx(props.queryModel, props.cubeId));

		const toggleEdit = function () {
			copyToClipboardStatus.value = "";

			isEditing.value = !isEditing.value;
		};

		const errorWithJson = ref("");
		const loadFromJson = function () {
			copyToClipboardStatus.value = "";

			try {
				const editedJsonParsed = JSON.parse(editedJson.value);

				const newQueryModel = {};

				newQueryModel.selectedColumns = {};
				if (editedJsonParsed.groupBy && editedJsonParsed.groupBy.columns) {
					for (const columnName of editedJsonParsed.groupBy.columns) {
						newQueryModel.selectedColumns[columnName] = true;
					}
				}

				newQueryModel.selectedMeasures = {};
				if (editedJsonParsed.measures) {
					for (const measureName of editedJsonParsed.measures) {
						newQueryModel.selectedMeasures[measureName] = true;
					}
				}

				newQueryModel.customMarkers = editedJsonParsed.customMarkers || {};

				newQueryModel.options = {};
				for (const option of editedJsonParsed.options) {
					newQueryModel.options[option] = true;
				}

				// We seemingly successfully loaded a modal: let's load it
				{
					props.queryModel.selectedColumnsOrdered = [];
					Object.assign(props.queryModel, newQueryModel);

					for (const columnName of editedJsonParsed.groupBy.columns) {
						props.queryModel.onColumnToggled(columnName);
					}
				}

				isEditing.value = false;
			} catch (error) {
				console.warn("Issue loading queryModel from JSON", error);
				errorWithJson.value = JSON.stringify(error);
			}
		};

		const copyToClipboard = function () {
			// Copy whatever the user is currently looking at — editing JSON copies the edited
			// buffer, viewing JSON copies the serialized queryJson, viewing MDX copies the MDX.
			let value;
			if (activeTab.value === "mdx") {
				value = mdxString.value;
			} else if (isEditing.value) {
				value = editedJson.value;
			} else {
				value = JSON.stringify(props.queryJson, null, 2);
			}
			console.log("Writing to clipboard");

			// https://stackoverflow.com/questions/51805395/navigator-clipboard-is-undefined
			if (!navigator.clipboard || !window.isSecureContext) {
				copyToClipboardStatus.value = "cancelled (https?)";
				return;
			}

			copyToClipboardStatus.value = "doing";
			navigator.clipboard
				.writeText(value)
				.then(() => {
					copyToClipboardStatus.value = "done";
				})
				.catch((error) => {
					console.error("Failed to copy to clipboard:", error);
					copyToClipboardStatus.value = "error";
				});
		};

		return {
			activeTab,
			mdxString,

			isEditing,
			toggleEdit,
			editedJson,

			loadFromJson,

			copyToClipboard,
			copyToClipboardStatus,
			errorWithJson,
		};
	},
	template: /* HTML */ `
		<!-- Button trigger modal -->
		<button type="button" class="btn btn-primary  btn-sm" data-bs-toggle="modal" data-bs-target="#queryJsonRaw">JSON</button>

		<!-- Modal -->
		<div class="modal fade" id="queryJsonRaw" tabindex="-1" aria-labelledby="exampleModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered modal-xl">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="exampleModalLabel">Query</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<!-- https://stackoverflow.com/questions/4611591/code-vs-pre-vs-samp-for-inline-and-block-code-snippets -->
					<div class="modal-body">
						<!--
							Tab switch: JSON (round-trippable, editable) vs MDX (derived, read-only).
							Nav-tabs is the Bootstrap-native pattern; keeps the component dependency-free.
						-->
						<ul class="nav nav-tabs mb-2">
							<li class="nav-item">
								<button
									type="button"
									class="nav-link"
									:class="activeTab === 'json' ? 'active' : ''"
									@click="activeTab = 'json'"
								>
									JSON
								</button>
							</li>
							<li class="nav-item">
								<button
									type="button"
									class="nav-link"
									:class="activeTab === 'mdx' ? 'active' : ''"
									@click="activeTab = 'mdx'"
								>
									MDX
								</button>
							</li>
						</ul>
						<!-- JSON tab: original view + edit mode. -->
						<div v-if="activeTab === 'json'">
							<!--
								Mode indicator bar: makes the read-only vs. editing state unmissable so the
								JSON <pre> area doesn't look like a broken textarea on desktop, nor like a
								tappable field on mobile (where hover cursors are absent).
							-->
							<div
								class="d-flex align-items-center justify-content-between border rounded px-2 py-1 mb-2 small"
								:class="isEditing ? 'bg-warning-subtle text-warning-emphasis' : 'bg-secondary-subtle text-secondary-emphasis'"
							>
								<span>
									<i v-if="isEditing" class="bi bi-pencil-square"></i>
									<i v-else class="bi bi-lock"></i>
									{{ isEditing ? "Editing — click Save to apply, Cancel to discard." : "Read-only — click Edit JSON below to modify." }}
								</span>
							</div>

							<div class="vh-50">
								<!--
									Editing: real textarea, normal cursor. Read-only: cursor: not-allowed
									+ subtle grey background + user-select: text kept ON so copy-paste still
									works. The overall styling reads as "disabled field" on every platform.
								-->
								<pre
									class="border text-start h-100 w-100"
									style=" overflow-y: scroll;"
									v-if="isEditing"
								><textarea class="h-100 w-100 px-0 py-0 border-0" style="box-sizing: content-box;" v-model="editedJson">irrelevant</textarea></pre>
								<pre
									v-else
									class="border text-start h-100 w-100 bg-body-secondary"
									style="overflow-y: scroll; cursor: not-allowed;"
									title="Read-only — click Edit JSON to modify"
									>{{queryJson}}</pre
								>
							</div>
						</div>

						<!--
							MDX tab: read-only derived view. Recomputes from queryModel whenever the
							model changes; no round-trip back to queryModel (out of scope for the
							minimal converter — see adhoc-query-to-mdx.js).
						-->
						<div v-else-if="activeTab === 'mdx'">
							<div class="d-flex align-items-center border rounded px-2 py-1 mb-2 small bg-secondary-subtle text-secondary-emphasis">
								<i class="bi bi-lock me-1"></i>
								Read-only — derived from the current queryModel. Use the clipboard button to copy.
							</div>
							<div class="vh-50">
								<pre
									class="border text-start h-100 w-100 bg-body-secondary"
									style="overflow-y: scroll; cursor: not-allowed;"
									title="Read-only — MDX derived from the queryModel"
									>{{mdxString}}</pre
								>
							</div>
						</div>
					</div>
					<div class="modal-footer">
						<!-- Edit controls only make sense on the JSON tab; hidden on MDX. -->
						<span v-if="activeTab === 'json' && isEditing">
							<button type="button" class="btn btn-success" @click="loadFromJson">Save</button>
							<button type="button" class="btn btn-warning" @click="toggleEdit">Cancel</button>
							<div v-if="errorWithJson.length >= 1">{{errorWithJson}}</div>
						</span>
						<span v-else-if="activeTab === 'json'">
							<button type="button" class="btn btn-primary" @click="toggleEdit">Edit JSON</button>
						</span>

						<button type="button" class="btn btn-light" @click="copyToClipboard"><i class="bi bi-clipboard"></i>{{copyToClipboardStatus}}</button>
						<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
