import { ref } from "vue";

import { mkConfig, generateCsv, download } from "https://cdn.jsdelivr.net/npm/export-to-csv@1.4.0/output/index.min.js";

// https://stackoverflow.com/questions/15900485/correct-way-to-convert-size-in-bytes-to-kb-mb-gb-in-javascript
function formatBytes(a, b = 2) {
	if (!+a) return "0 Bytes";
	const c = 0 > b ? 0 : b,
		d = Math.floor(Math.log(a) / Math.log(1024));
	return `${parseFloat((a / Math.pow(1024, d)).toFixed(c))} ${["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"][d]}`;
}

export default {
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		array: {
			type: Array,
			required: true,
		},
	},
	setup(props) {
		const csvConfig = function () {
			// mkConfig merges your options with the defaults
			// and returns WithDefaults<ConfigOptions>
			const csvConfig = mkConfig({ useKeysAsHeaders: true });

			return csvConfig;
		};

		const toCsv = function () {
			// Converts your Array<Object> to a CsvOutput string based on the configs
			const csv = generateCsv(csvConfig())(props.array);

			return csv;
		};

		// https://github.com/alexcaza/export-to-csv?tab=readme-ov-file#in-browser
		const downloadAsCsv = function () {
			const csv = toCsv();

			console.log("Downloading csv with size", formatBytes(csv.length));

			// Add a click handler that will run the `download` function.
			// `download` takes `csvConfig` and the generated `CsvOutput`
			// from `generateCsv`.
			download(csvConfig())(csv);
		};

		const copyToClipboardStatus = ref("");
		const copyToClipboard = function () {
			const csv = toCsv();

			console.log("Writing to clipboard size", formatBytes(csv.length));

			// https://stackoverflow.com/questions/51805395/navigator-clipboard-is-undefined
			if (!navigator.clipboard || !window.isSecureContext) {
				copyToClipboardStatus.value = "cancelled (https?)";
				return;
			}

			copyToClipboardStatus.value = "doing";
			navigator.clipboard
				.writeText(csv)
				.then(() => {
					copyToClipboardStatus.value = "done";
				})
				.catch((error) => {
					console.error("Failed to copy to clipboard:", error);
					copyToClipboardStatus.value = "error";
				});
		};

		return {
			downloadAsCsv,

			copyToClipboard,
			copyToClipboardStatus,
		};
	},
	template: /* HTML */ `
		<!--
			Export entry point. Wraps the two export channels (download as a .csv file /
			copy CSV text to the clipboard) behind a single dropdown so the grid's bottom
			control strip stays compact. The component is self-contained: the btn-group
			below uses Bootstrap's native dropdown JS, auto-initialised via the HTML
			attribute data-bs-toggle=dropdown (bootstrap.esm.js is already loaded via the
			import-map in index.html).
		-->
		<div v-if="array.length >= 1" class="btn-group">
			<button
				type="button"
				class="btn btn-primary btn-sm dropdown-toggle"
				data-bs-toggle="dropdown"
				aria-expanded="false"
				title="Export the current grid"
			>
				<i class="bi bi-box-arrow-up me-1"></i>Export
			</button>
			<ul class="dropdown-menu">
				<li>
					<button type="button" class="dropdown-item" @click="downloadAsCsv">
						<i class="bi bi-download me-1"></i>Download CSV
					</button>
				</li>
				<li>
					<button type="button" class="dropdown-item" @click="copyToClipboard">
						<i class="bi bi-clipboard me-1"></i>Copy CSV to clipboard
						<span v-if="copyToClipboardStatus" class="small text-muted ms-1">{{copyToClipboardStatus}}</span>
					</button>
				</li>
			</ul>
		</div>
	`,
};
