import { ref, computed, watch, inject } from "vue";

import mermaid from "mermaid";

import { useAdhocStore } from "./store-adhoc.js";
import { queryModelToMdx } from "./adhoc-query-to-mdx.js";
import { queryModelToMermaid } from "./adhoc-query-to-mermaid.js";
import { queryModelToSql } from "./adhoc-query-to-sql.js";

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
		// behaviour (with edit mode); "mdx" and "sql" show derived strings for side-by-side
		// comparison with external MDX / SQL clients. Both derived views are informative only
		// and always read-only — Adhoc executes the JSON model, not the MDX or SQL text.
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

		// Resolve the cube's measure definitions (lazy — may be missing on first render if the
		// schema hasn't loaded yet) so the SQL converter can upgrade `.Aggregator` measures from
		// bare identifiers to their real SQL aggregate (SUM("col") AS "name", …). `ids` is
		// provided by `adhoc-query.js` one level up.
		const ids = inject("ids", null);
		const store = useAdhocStore();
		const measureDefs = computed(() => {
			if (!ids) return {};
			return store.schemas[ids.endpointId]?.cubes[ids.cubeId]?.measures || {};
		});

		// Live SQL projection of the current queryModel — same informative-only contract as MDX.
		const sqlString = computed(() => queryModelToSql(props.queryModel, props.cubeId, measureDefs.value));

		// Live Mermaid flowchart projection — a sandwich of Filter → Cube → GroupBy → Measures.
		// Informative-only: the diagram is a reading aid for the current queryModel, NOT an
		// execution plan.
		const mermaidSource = computed(() => queryModelToMermaid(props.queryModel, props.cubeId));

		// Mermaid is a one-off initialisation per page load. `startOnLoad: false` keeps the
		// library passive — we call `render()` ourselves below so the SVG lives in a Vue-managed
		// fragment rather than Mermaid hunting for `.mermaid` elements in the DOM.
		mermaid.initialize({ startOnLoad: false, securityLevel: "loose", logLevel: 5 });

		// Rendered SVG as a reactive ref. Recomputes whenever the Mermaid tab becomes active OR
		// the mermaidSource changes — both triggers are wired via the watcher below.
		const mermaidSvg = ref("");
		const mermaidError = ref("");
		let mermaidRenderId = 0;
		const renderMermaid = async function (source) {
			if (!source) {
				mermaidSvg.value = "";
				mermaidError.value = "";
				return;
			}
			try {
				// Fresh id per render so Mermaid doesn't reuse a stale SVG node. Monotonic
				// counter avoids DOM-id collisions when the user switches tabs quickly.
				const id = "queryMermaid_" + ++mermaidRenderId;
				const res = await mermaid.render(id, source);
				mermaidSvg.value = res.svg;
				mermaidError.value = "";
			} catch (e) {
				mermaidError.value = String(e.message || e);
				mermaidSvg.value = "";
			}
		};
		watch(
			[activeTab, mermaidSource],
			([tab, source]) => {
				if (tab === "mermaid") renderMermaid(source);
			},
			{ immediate: false },
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
			// Copy whatever the user is currently looking at — editing JSON copies the edited
			// buffer, viewing JSON copies the serialized queryJson, viewing MDX/SQL copies the
			// derived string.
			let value;
			if (activeTab.value === "mdx") {
				value = mdxString.value;
			} else if (activeTab.value === "sql") {
				value = sqlString.value;
			} else if (activeTab.value === "mermaid") {
				// Copy the Mermaid source (not the rendered SVG) so the user can paste at
				// mermaid.live or into markdown for sharing.
				value = mermaidSource.value;
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
			sqlString,
			mermaidSource,
			mermaidSvg,
			mermaidError,

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

		<!--
			Teleport the modal to body. When this component is rendered inside the floating
			Submit block (which uses transform: translate(...) for centering), the transform
			creates a new containing block for fixed-position descendants. The Bootstrap
			modal-backdrop and modal would otherwise be sized/positioned relative to that
			transformed parent — the visible result is a greyed-out backdrop that cannot be
			interacted with and whose tab strip is off-screen. Teleporting to body escapes
			the containing block so the modal lays out against the viewport as Bootstrap
			expects.
		-->
		<Teleport to="body">
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
										data-adhoc-tab="json"
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
										data-adhoc-tab="mdx"
									>
										MDX
									</button>
								</li>
								<li class="nav-item">
									<button
										type="button"
										class="nav-link"
										:class="activeTab === 'sql' ? 'active' : ''"
										@click="activeTab = 'sql'"
										data-adhoc-tab="sql"
									>
										SQL
									</button>
								</li>
								<li class="nav-item">
									<button
										type="button"
										class="nav-link"
										:class="activeTab === 'mermaid' ? 'active' : ''"
										@click="activeTab = 'mermaid'"
										data-adhoc-tab="mermaid"
									>
										Mermaid
									</button>
								</li>
							</ul>
							<!-- JSON tab: original view + edit mode. -->
							<div v-if="activeTab === 'json'" data-adhoc-tab-pane="json">
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
									>
{{queryJson}}</pre
									>
								</div>
							</div>

							<!--
								MDX tab: read-only derived view. Recomputes from queryModel whenever the
								model changes; no round-trip back to queryModel (out of scope for the
								minimal converter — see adhoc-query-to-mdx.js).
							-->
							<div v-else-if="activeTab === 'mdx'" data-adhoc-tab-pane="mdx">
								<!--
									Informative-only notice. This MDX string is NOT what Adhoc runs — the
									backend evaluates the JSON queryModel. We surface the MDX projection so
									the user can copy/paste it into a real MDX client (Excel, Mondrian,
									Atoti, …) as a starting point when translating the query by hand.
								-->
								<div class="alert alert-info py-2 mb-2 small" role="alert">
									<i class="bi bi-info-circle me-1"></i>
									<strong>Informative only.</strong>
									This MDX is derived from the current query model — Adhoc does <em>not</em> execute MDX. Use it as a starting point to build
									an actual MDX query for an MDX-native client (Excel, Mondrian, Atoti, …).
								</div>
								<div class="vh-50">
									<pre
										class="border text-start h-100 w-100 bg-body-secondary"
										style="overflow-y: scroll; cursor: not-allowed;"
										title="Read-only — MDX derived from the queryModel (informative only)"
									>
{{mdxString}}</pre
									>
								</div>
							</div>

							<!--
								Mermaid tab: visual sandwich of Filter -> Cube -> GroupBy -> Measures.
								Renders the SVG lazily when the tab is activated (see watcher in setup).
								The Mermaid source text is shown below the rendered SVG so the user can
								copy it to mermaid.live or into markdown for sharing — same informative-
								only contract as the MDX / SQL tabs.
							-->
							<div v-else-if="activeTab === 'mermaid'" data-adhoc-tab-pane="mermaid">
								<div class="alert alert-info py-2 mb-2 small" role="alert">
									<i class="bi bi-info-circle me-1"></i>
									<strong>Informative only.</strong>
									A visual projection of the current query model — Filter -> Cube -> GroupBy -> Measures. Useful to read complex filter trees
									at a glance. The source text below the diagram can be pasted at
									<a href="https://mermaid.live" target="_blank" rel="noopener">mermaid.live</a>
									or into any markdown viewer that supports Mermaid.
								</div>
								<div v-if="mermaidError.length >= 1" class="alert alert-warning small">Mermaid render error: {{mermaidError}}</div>
								<div class="border p-2 mb-2 text-center bg-body-secondary" style="overflow-x: auto;" v-html="mermaidSvg"></div>
								<pre
									class="border text-start w-100 bg-body-secondary small"
									style="max-height: 200px; overflow-y: scroll; cursor: not-allowed;"
									title="Read-only — Mermaid source derived from the queryModel (informative only)"
								>
{{mermaidSource}}</pre
								>
							</div>

							<!--
								SQL tab: same informative-only contract as MDX. Recomputes from queryModel
								via adhoc-query-to-sql.js; no round-trip back to queryModel.
							-->
							<div v-else-if="activeTab === 'sql'" data-adhoc-tab-pane="sql">
								<div class="alert alert-info py-2 mb-2 small" role="alert">
									<i class="bi bi-info-circle me-1"></i>
									<strong>Informative only.</strong>
									This SQL is derived from the current query model — Adhoc does <em>not</em> execute SQL. Use it as a starting point to build
									an actual SQL query for a SQL-native client (DuckDB, Postgres, BigQuery, …).
								</div>
								<div class="vh-50">
									<pre
										class="border text-start h-100 w-100 bg-body-secondary"
										style="overflow-y: scroll; cursor: not-allowed;"
										title="Read-only — SQL derived from the queryModel (informative only)"
									>
{{sqlString}}</pre
									>
								</div>
							</div>
						</div>
						<div class="modal-footer">
							<!-- Edit controls only make sense on the JSON tab; hidden on MDX / SQL. -->
							<span v-if="activeTab === 'json' && isEditing">
								<button type="button" class="btn btn-success" @click="loadFromJson">Save</button>
								<button type="button" class="btn btn-warning" @click="toggleEdit">Cancel</button>
								<div v-if="errorWithJson.length >= 1">{{errorWithJson}}</div>
							</span>
							<span v-else-if="activeTab === 'json'">
								<button type="button" class="btn btn-primary" @click="toggleEdit">Edit JSON</button>
							</span>

							<button type="button" class="btn btn-light" @click="copyToClipboard">
								<i class="bi bi-clipboard"></i>{{copyToClipboardStatus}}
							</button>
							<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
						</div>
					</div>
				</div>
			</div>
		</Teleport>
	`,
};
