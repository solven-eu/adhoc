import { ref, watch, onMounted, reactive } from "vue";

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
        <span v-if="array.length >= 1">
            <button type="button" class="btn btn-primary" @click="downloadAsCsv">Download CSV</button>
            <button type="button" class="btn btn-primary" @click="copyToClipboard">
                <i class="bi bi-clipboard"></i>Copy to clipboard {{copyToClipboardStatus}}
            </button>
        </span>
    `,
};
