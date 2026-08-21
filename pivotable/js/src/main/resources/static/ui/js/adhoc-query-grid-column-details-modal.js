// @ts-check
import { computed } from "vue";

import { describeTag } from "./adhoc-baked-in-tags.js";

// Per-column details modal. Triggered from the grid groupBy column header menu — see
// `adhoc-query-grid-helper.js` — mirroring the measure side's "Show DAG" and Statistics modals.
//
// Answers "what IS this column": what else it is called (aliases), what it holds (type, tags),
// and what values it takes (estimated cardinality plus a sample of members). Those come from
// two different places: the cube schema already in the store, and the `/schemas/columns`
// endpoint which is fetched on open — hence the loading state.
export default {
	props: {
		detailsModel: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		/** Members are a sample, never guaranteed exhaustive — the label has to say so when it is truncated. */
		const coordinates = computed(() => {
			const loaded = props.detailsModel.details;
			return (loaded && loaded.coordinates) || [];
		});

		const estimatedCardinality = computed(() => {
			const loaded = props.detailsModel.details;
			if (!loaded || typeof loaded.estimatedCardinality !== "number" || loaded.estimatedCardinality < 0) {
				return null;
			}
			return loaded.estimatedCardinality;
		});

		// True when the sample shows fewer members than the column is estimated to hold, so the list must not read as
		// the complete set.
		const isTruncated = computed(() => {
			const total = estimatedCardinality.value;
			return typeof total === "number" && coordinates.value.length < total;
		});

		const tags = computed(() => {
			const loaded = props.detailsModel.details;
			return (loaded && loaded.tags) || [];
		});

		const aliases = computed(() => {
			const loaded = props.detailsModel.details;
			return (loaded && loaded.aliases) || [];
		});

		const describe = function (tag) {
			return describeTag(tag, props.detailsModel.tagDescriptions);
		};

		return { coordinates, estimatedCardinality, isTruncated, tags, aliases, describe };
	},
	template: /* HTML */ `
		<div class="modal fade" id="columnDetailsModal" tabindex="-1" aria-labelledby="columnDetailsModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered modal-lg">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="columnDetailsModalLabel">
							<i class="bi bi-info-circle me-2"></i>Column — <span class="font-monospace">{{detailsModel.column}}</span>
						</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<div v-if="detailsModel.loading" class="d-flex align-items-center gap-2 text-muted">
							<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
							<span>Loading column details…</span>
						</div>
						<div v-else-if="detailsModel.error" class="alert alert-warning mb-0">
							Could not load details for this column: {{detailsModel.error}}
						</div>
						<table v-else class="table table-sm align-middle">
							<tbody>
								<tr>
									<th class="text-muted" style="width: 30%">Holder</th>
									<td>
										<span class="font-monospace">{{detailsModel.details?.holder || '—'}}</span>
										<small class="text-muted ms-2">the cube or table answering for this column</small>
									</td>
								</tr>
								<tr>
									<th class="text-muted">Type</th>
									<td class="font-monospace">{{detailsModel.details?.type || '—'}}</td>
								</tr>
								<tr>
									<th class="text-muted">Aliases</th>
									<td>
										<span v-if="aliases.length === 0" class="text-muted">none reported</span>
										<span v-else>
											<span v-for="alias in aliases" class="badge text-bg-light font-monospace me-1">{{alias}}</span>
										</span>
									</td>
								</tr>
								<tr>
									<th class="text-muted">Tags</th>
									<td>
										<span v-if="tags.length === 0" class="text-muted">none</span>
										<span v-else>
											<span v-for="tag in tags" class="badge text-bg-secondary me-1" :title="describe(tag)">{{tag}}</span>
										</span>
									</td>
								</tr>
								<tr>
									<th class="text-muted">Distinct members</th>
									<td class="font-monospace">{{estimatedCardinality === null ? 'not estimated' : estimatedCardinality}}</td>
								</tr>
								<tr>
									<th class="text-muted">Members</th>
									<td>
										<span v-if="coordinates.length === 0" class="text-muted">none sampled</span>
										<div v-else>
											<span v-for="coordinate in coordinates" class="badge text-bg-light font-monospace me-1 mb-1">
												{{coordinate === null ? 'null' : coordinate}}
											</span>
											<div v-if="isTruncated" class="text-muted small mt-1">
												Showing {{coordinates.length}} of {{estimatedCardinality}} — this is a sample, not the full set.
											</div>
										</div>
									</td>
								</tr>
							</tbody>
						</table>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
