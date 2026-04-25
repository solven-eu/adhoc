import { ref } from "vue";

import { mapState } from "pinia";
import { useAdhocStore } from "./store-adhoc.js";

// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
import { Tooltip } from "bootstrap";

import AdhocEndpointSchemaRef from "./adhoc-endpoint-schema-ref.js";

import AdhocCube from "./adhoc-cube.js";
import AdhocCubeRef from "./adhoc-cube-ref.js";

import AdhocLoading from "./adhoc-loading.js";

export default {
	components: {
		AdhocEndpointSchemaRef,
		AdhocCube,
		AdhocCubeRef,
		AdhocLoading,
	},
	props: {
		endpointId: {
			type: String,
			required: true,
		},
		cubeId: {
			type: String,
			required: false,
		},
		showSchema: {
			type: Boolean,
			default: false,
		},
	},
	computed: {
		...mapState(useAdhocStore, ["nbSchemaFetching", "metadata"]),
		...mapState(useAdhocStore, {
			schema(store) {
				return store.schemas[this.endpointId] || { error: "not_loaded" };
			},
		}),
	},
	setup(props) {
		const store = useAdhocStore();

		const nbCubes = ref("...");

		const percentUi = ref(0);

		store
			.loadEndpointSchemaIfMissing(props.endpointId, (value, done, percent) => {
				percentUi.value = percent;
			})
			.then((schema) => {
				var endpointSchema = schema || { cubes: {} };
				nbCubes.value = Object.keys(endpointSchema.cubes).length;
			});

		// https://getbootstrap.com/docs/5.3/components/tooltips/
		// https://stackoverflow.com/questions/69053972/adding-bootstrap-5-tooltip-to-vue-3
		// NOSONAR
		new Tooltip(document.body, { selector: "[data-bs-toggle='tooltip']" });

		// Per-section search inputs. The schema page used to be a flat dump of every
		// table / measure-bag / cube which got hard to read on cubes with > 50 measures.
		// A small case-insensitive filter per section lets the reader narrow down without
		// scrolling.
		const tableFilter = ref("");
		const measureFilter = ref("");
		const cubeFilter = ref("");

		const lowerIncludes = function (haystack, needle) {
			if (!needle) return true;
			return String(haystack || "")
				.toLowerCase()
				.includes(needle.trim().toLowerCase());
		};

		// Pretty-print one measure entry. Centralised here so the template stays simple
		// and the format is consistent across measure types. Unknown types fall back to
		// JSON-stringify so nothing renders as `[object Object]`.
		const measureSignature = function (m) {
			if (!m || !m.type) return JSON.stringify(m);
			if (m.type === ".Aggregator") {
				return (m.aggregationKey || "?") + "(" + (m.columnName || "?") + ")";
			}
			if (m.type === ".Combinator") {
				return (m.combinationKey || "?") + "(" + (m.underlyings || []).join(", ") + ")";
			}
			if (m.type === ".Filtrator") {
				return "filter(" + (m.underlying || "?") + ")";
			}
			if (m.type === ".Dispatchor" || m.type === ".Partitionor") {
				return (m.decompositionKey || m.editorKey || "?") + "(" + (m.underlyings || [m.underlying] || []).filter((u) => u).join(", ") + ")";
			}
			return JSON.stringify(m);
		};

		const measureTypeBadge = function (type) {
			// Map measure type → bootstrap badge color so the eye can scan a long list of
			// mixed measures and group them by kind without reading the full type string.
			switch (type) {
				case ".Aggregator":
					return { label: "Aggregator", cls: "text-bg-primary" };
				case ".Combinator":
					return { label: "Combinator", cls: "text-bg-info" };
				case ".Filtrator":
					return { label: "Filtrator", cls: "text-bg-success" };
				case ".Dispatchor":
					return { label: "Dispatchor", cls: "text-bg-secondary" };
				case ".Partitionor":
					return { label: "Partitionor", cls: "text-bg-warning" };
				default:
					return { label: (type || "?").replace(/^\./, ""), cls: "text-bg-light" };
			}
		};

		return { nbCubes, percentUi, tableFilter, measureFilter, cubeFilter, lowerIncludes, measureSignature, measureTypeBadge };
	},
	template: /* HTML */ `
		<div v-if="!schema || schema.error">
			<AdhocLoading :id="endpointId" type="schema" :loading="nbSchemaFetching > 0" :error="schema.error" />

			{{percentUi}}
			<div
				class="progress"
				role="progressbar"
				aria-label="Animated striped example"
				:aria-valuenow="percentUi * 100"
				aria-valuemin="0"
				aria-valuemax="100"
			>
				<div class="progress-bar progress-bar-striped progress-bar-animated" :style="'width: ' + (percentUi * 100) + '%'"></div>
			</div>
		</div>
		<div v-else>
			<!--
				Three-section accordion replacing the previous flat list. Each section header
				surfaces a count badge so the reader has the rough shape of the schema at a
				glance; expanding a section reveals a small per-section filter and a tidy
				inner accordion of entries. Cubes default to expanded (most-clicked
				navigation surface); Tables and Measures collapsed.
			-->
			<div v-if="showSchema" class="accordion" :id="'schemaAccordion_' + endpointId">
				<!-- TABLES -->
				<div class="accordion-item">
					<h2 class="accordion-header">
						<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" :data-bs-target="'#schemaTables_' + endpointId">
							<i class="bi bi-table me-2"></i>Tables
							<span class="badge text-bg-primary ms-2">{{Object.keys(schema.tables || {}).length}}</span>
						</button>
					</h2>
					<div :id="'schemaTables_' + endpointId" class="accordion-collapse collapse" :data-bs-parent="'#schemaAccordion_' + endpointId">
						<div class="accordion-body">
							<input type="text" class="form-control form-control-sm mb-2" placeholder="Filter tables…" v-model="tableFilter" />
							<div v-if="!schema.tables || Object.keys(schema.tables).length === 0" class="text-muted small">No tables.</div>
							<div v-else class="accordion accordion-flush" :id="'tablesInner_' + endpointId">
								<template v-for="(table, name) in schema.tables" :key="name">
									<div v-if="lowerIncludes(name, tableFilter)" class="accordion-item">
										<h2 class="accordion-header">
											<button
												class="accordion-button collapsed py-2"
												type="button"
												data-bs-toggle="collapse"
												:data-bs-target="'#table_' + endpointId + '_' + name"
											>
												<span class="font-monospace small">{{name}}</span>
												<span class="badge text-bg-light ms-2">{{Object.keys(table.columnToTypes || {}).length}} cols</span>
											</button>
										</h2>
										<div :id="'table_' + endpointId + '_' + name" class="accordion-collapse collapse">
											<div class="accordion-body py-2">
												<table class="table table-sm table-borderless mb-0">
													<tbody>
														<tr v-for="(typ, col) in table.columnToTypes" :key="col">
															<td class="font-monospace small">{{col}}</td>
															<td class="text-muted small text-end">{{typ}}</td>
														</tr>
													</tbody>
												</table>
											</div>
										</div>
									</div>
								</template>
							</div>
						</div>
					</div>
				</div>

				<!-- MEASURES -->
				<div class="accordion-item">
					<h2 class="accordion-header">
						<button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" :data-bs-target="'#schemaMeasures_' + endpointId">
							<i class="bi bi-rulers me-2"></i>Measure bags
							<span class="badge text-bg-primary ms-2">{{Object.keys(schema.measureBags || {}).length}}</span>
						</button>
					</h2>
					<div :id="'schemaMeasures_' + endpointId" class="accordion-collapse collapse" :data-bs-parent="'#schemaAccordion_' + endpointId">
						<div class="accordion-body">
							<input
								type="text"
								class="form-control form-control-sm mb-2"
								placeholder="Filter measures (matches bag name or measure name)…"
								v-model="measureFilter"
							/>
							<div v-if="!schema.measureBags || Object.keys(schema.measureBags).length === 0" class="text-muted small">No measure bags.</div>
							<div v-else class="accordion accordion-flush">
								<template v-for="(measureBag, bagName) in schema.measureBags" :key="bagName">
									<div
										v-if="lowerIncludes(bagName, measureFilter) || (measureBag || []).some((m) => lowerIncludes(m.name, measureFilter))"
										class="accordion-item"
									>
										<h2 class="accordion-header">
											<button
												class="accordion-button collapsed py-2"
												type="button"
												data-bs-toggle="collapse"
												:data-bs-target="'#bag_' + endpointId + '_' + bagName"
											>
												<span class="font-monospace small">{{bagName}}</span>
												<span class="badge text-bg-light ms-2">{{(measureBag || []).length}} measures</span>
											</button>
										</h2>
										<div :id="'bag_' + endpointId + '_' + bagName" class="accordion-collapse collapse">
											<div class="accordion-body py-2">
												<table class="table table-sm table-borderless mb-0">
													<tbody>
														<tr v-for="m in (measureBag || [])" :key="m.name">
															<td class="small">
																<span class="badge me-2" :class="measureTypeBadge(m.type).cls"
																	>{{measureTypeBadge(m.type).label}}</span
																>
															</td>
															<td class="font-monospace small">{{m.name}}</td>
															<td class="font-monospace small text-muted">{{measureSignature(m)}}</td>
														</tr>
													</tbody>
												</table>
											</div>
										</div>
									</div>
								</template>
							</div>
						</div>
					</div>
				</div>

				<!-- CUBES -->
				<div class="accordion-item">
					<h2 class="accordion-header">
						<button class="accordion-button" type="button" data-bs-toggle="collapse" :data-bs-target="'#schemaCubes_' + endpointId">
							<i class="bi bi-grid-3x3-gap me-2"></i>Cubes
							<span class="badge text-bg-primary ms-2">{{Object.keys(schema.cubes || {}).length}}</span>
						</button>
					</h2>
					<div :id="'schemaCubes_' + endpointId" class="accordion-collapse collapse show" :data-bs-parent="'#schemaAccordion_' + endpointId">
						<div class="accordion-body">
							<input type="text" class="form-control form-control-sm mb-2" placeholder="Filter cubes…" v-model="cubeFilter" />
							<div v-if="!schema.cubes || Object.keys(schema.cubes).length === 0" class="text-muted small">No cubes.</div>
							<ul v-else class="list-group">
								<template v-for="(cube, cubeName) in schema.cubes" :key="cubeName">
									<li v-if="lowerIncludes(cubeName, cubeFilter)" class="list-group-item d-flex flex-wrap align-items-center gap-2">
										<AdhocCubeRef :endpointId="endpointId" :cubeId="cubeName" />
										<span class="badge text-bg-light" v-if="cube.columns &amp;&amp; cube.columns.columnToTypes">
											{{Object.keys(cube.columns.columnToTypes).length}} cols
										</span>
										<span class="badge text-bg-light" v-if="cube.measureBag">{{cube.measureBag}}</span>
									</li>
								</template>
							</ul>
						</div>
					</div>
				</div>
			</div>
			<span v-else>
				<div>
					Tables:
					<span v-for="(table, name) in schema.tables"> {{name}} &nbsp;</span>
				</div>
				<div>
					Measures
					<span v-for="(measureBag, name) in schema.measureBags"> {{name}} &nbsp;</span>
				</div>

				<div>
					Cubes
					<span v-for="(cube, cubeName) in schema.cubes"> <AdhocCubeRef :endpointId="endpointId" :cubeId="cubeName" />&nbsp; </span>
				</div>
				<AdhocEndpointSchemaRef :endpointId="endpointId" />
			</span>
		</div>
	`,
};
