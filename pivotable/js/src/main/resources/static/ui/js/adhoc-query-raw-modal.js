import { ref, watch } from "vue";

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
	},
	setup(props) {
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
			const jsonValue = isEditing.value ? editedJson.value : JSON.stringify(props.queryJson, null, 2);
			console.log("Writing to clipboard");
			copyToClipboardStatus.value = "doing";
			navigator.clipboard
				.writeText(jsonValue)
				.then(() => {
					copyToClipboardStatus.value = "done";
				})
				.catch((error) => {
					console.error("Failed to copy to clipboard:", error);
					copyToClipboardStatus.value = "error";
				});
		};

		return {
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
                        <h5 class="modal-title" id="exampleModalLabel">Query as JSON</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <!-- https://stackoverflow.com/questions/4611591/code-vs-pre-vs-samp-for-inline-and-block-code-snippets -->
                    <div class="modal-body">
                        <div class="vh-50">
                            <pre
                                class="border text-start h-100 w-100"
                                style=" overflow-y: scroll;"
                                v-if="isEditing"
                            ><textarea class="h-100 w-100 px-0 py-0 border-0" style="box-sizing: content-box;" v-model="editedJson">irrelevant</textarea></pre>
                            <pre class="border text-start h-100 w-100" style="overflow-y: scroll;" v-else>{{queryJson}}</pre>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <span v-if="isEditing">
                            <button type="button" class="btn btn-success" @click="loadFromJson">Save</button>
                            <button type="button" class="btn btn-warning" @click="toggleEdit">Cancel</button>
                            <div v-if="errorWithJson.length >= 1">{{errorWithJson}}</div>
                        </span>
                        <span v-else>
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
