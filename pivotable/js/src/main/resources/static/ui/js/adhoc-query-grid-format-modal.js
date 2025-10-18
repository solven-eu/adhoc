import AdhocGridFormat from "./adhoc-query-grid-format.js";

export default {
	components: {
		AdhocGridFormat,
	},
	props: {
		formatOptions: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		return {};
	},
	template: /* HTML */ `
        <!-- Button trigger modal -->
        <button type="button" class="btn btn-primary  btn-sm" data-bs-toggle="modal" data-bs-target="#gridFormatOptions">Formatting Options</button>

        <!-- Modal -->
        <div class="modal fade" id="gridFormatOptions" tabindex="-1" aria-labelledby="exampleModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="exampleModalLabel">Formatting options</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <!-- https://stackoverflow.com/questions/4611591/code-vs-pre-vs-samp-for-inline-and-block-code-snippets -->
                    <div class="modal-body">
                        <AdhocGridFormat :formatOptions="formatOptions" />
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        </div>
    `,
};
