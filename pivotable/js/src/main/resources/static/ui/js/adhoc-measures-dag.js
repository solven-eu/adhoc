import { inject, ref, watch } from "vue";
import mermaid from "mermaid";

export default {
	props: {
		measuresDagModel: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const cube = inject("cube");
		const queryModel = inject("queryModel");

		// https://github.com/hojas/vue-mermaid-render/blob/main/packages/vue-mermaid-render/src/components/VueMermaidRender.vue
		const mermaidString = ref("");

		// Explicit clicks the user made INSIDE the modal. Accumulates while the modal is open and
		// gets reset each time the modal is opened on a fresh `main` measure (new `?` click in the
		// grid / wizard), so the user starts with a clean slate per entry point.
		const clickedMeasures = ref([]);

		// Optional mode: when on, `computeMarkdown` also pulls in every node that lies on a path
		// between any two user-selected nodes, so relations between disjoint selections become
		// visible without the user having to click every intermediate measure. Off by default
		// because on deep forests it can pull a large subgraph.
		const expandLinks = ref(false);

		mermaid.initialize({
			startOnLoad: false,
			securityLevel: "loose",
			// From 0 to 5 (0 is ALL, 5 is none)
			logLevel: 5,
		});

		// Mermaid renders SVG nodes with click handlers that call this function by name. The
		// function is attached to `window` so Mermaid's `click id href "javascript:..."` binding
		// can resolve it. Adds the measure to the queryModel (so the wizard/grid reflect it) AND
		// to `clickedMeasures` (so the modal graph expands to include it on the next tick).
		window.clickAddMeasure = (measure) => {
			console.info("Clicked m=", measure);
			queryModel.selectedMeasures[measure] = true;
			if (!clickedMeasures.value.includes(measure)) {
				clickedMeasures.value.push(measure);
			}
		};

		// Reverse the `measure->underlying` mapping, so we can show measures depending on a selected measure.
		const measureToDependants = {};
		const fillMeasureToDependants = function () {
			// Compute lazily as it is expensive
			function safeArray(m) {
				if (!measureToDependants[m]) {
					measureToDependants[m] = [];
				}
				return measureToDependants[m];
			}

			for (let oneMeasure of Object.keys(cube.measures)) {
				const measure = cube.measures[oneMeasure];
				if (measure.underlying) {
					safeArray(measure.underlying).push(oneMeasure);
				} else if (measure.underlyings) {
					for (const underlying of measure.underlyings) {
						safeArray(underlying).push(oneMeasure);
					}
				}
			}

			console.log(`Computed measureToDependants for ${Object.keys(measureToDependants).length} measures`);
		};

		const underlyingsOf = function (name) {
			const m = cube.measures[name];
			if (!m) return [];
			if (m.underlying) return [m.underlying];
			if (m.underlyings) return [...m.underlyings];
			return [];
		};

		// BFS over the directed edge set `from -> neighbours(from)`. Returns the set of nodes
		// reachable from `start` (inclusive), up to a safety cap to avoid runaway traversals on
		// degenerate graphs.
		const reachable = function (start, neighbours) {
			const visited = new Set();
			const queue = [start];
			const CAP = 10000;
			while (queue.length > 0 && visited.size < CAP) {
				const n = queue.shift();
				if (visited.has(n)) continue;
				visited.add(n);
				for (const nx of neighbours(n) || []) {
					if (!visited.has(nx)) queue.push(nx);
				}
			}
			return visited;
		};

		// Returns every node that lies on a path from any node in `selected` to any other node
		// in `selected`, following the measure DAG in either direction. Implementation: for each
		// pair (A, B), a node N is on a path A -> B iff it is reachable from A via underlyings
		// AND can reach B via dependants. We union both directions.
		const nodesOnPathsBetween = function (selected) {
			const onPaths = new Set();
			const selectedArray = Array.from(new Set(selected));
			for (const a of selectedArray) {
				const reachFromA = reachable(a, underlyingsOf);
				const reachToA = reachable(a, (n) => measureToDependants[n] || []);
				for (const b of selectedArray) {
					if (a === b) continue;
					const reachFromB = reachable(b, underlyingsOf);
					const reachToB = reachable(b, (n) => measureToDependants[n] || []);
					// Nodes on paths A -> B: descendants of A that can reach B.
					for (const n of reachFromA) {
						if (reachToB.has(n)) onPaths.add(n);
					}
					// Nodes on paths B -> A: descendants of B that can reach A.
					for (const n of reachFromB) {
						if (reachToA.has(n)) onPaths.add(n);
					}
				}
			}
			return onPaths;
		};

		// Recompute the set of user-selected nodes + their rendered graph. Called whenever the
		// dag model, the user's in-modal click list, or the "include links" toggle changes.
		const rebuildGraph = function () {
			const userSelected = new Set();
			if (props.measuresDagModel.main) userSelected.add(props.measuresDagModel.main);
			for (const m of props.measuresDagModel.highlight || []) userSelected.add(m);
			for (const m of clickedMeasures.value) userSelected.add(m);

			if (userSelected.size === 0) {
				updateGraph("flowchart TD\r\n    empty");
				return;
			}

			if (Object.keys(measureToDependants).length === 0) {
				fillMeasureToDependants();
			}

			const expanded = new Set(userSelected);
			if (expandLinks.value) {
				for (const n of nodesOnPathsBetween(userSelected)) {
					expanded.add(n);
				}
			}

			updateGraph(computeMarkdown(expanded, userSelected));
		};

		// Reset the in-modal click list whenever the user reopens the modal on a DIFFERENT main
		// measure. Without this reset, clicks would accumulate across unrelated `?` openings.
		watch(
			() => props.measuresDagModel.main,
			(newMain, oldMain) => {
				if (newMain !== oldMain) {
					clickedMeasures.value = [];
				}
			},
		);

		watch(
			[() => props.measuresDagModel, clickedMeasures, expandLinks],
			() => {
				rebuildGraph();
			},
			{ deep: true },
		);

		// generate svg id
		function genSvgId() {
			const max = 1000000;
			return `mermaid-svg-${genId(max)}${genId(max)}`;

			function genId(max) {
				return ~~(Math.random() * max);
			}
		}

		function computeMarkdown(expanded, userSelected) {
			let markdown = [];

			// `LR` renders better than `TD` when measures have many dependents.
			markdown.push("flowchart LR");

			// Mermaid node IDs must be simple identifiers — measure names can contain arbitrary
			// characters, so we map each name to a synthetic id.
			// https://github.com/mermaid-js/mermaid/issues/2388
			const nameToId = {};
			const ensureId = function (name) {
				if (nameToId[name] === undefined) {
					const measureId = "id" + Object.keys(nameToId).length;
					nameToId[name] = measureId;
				}
				return nameToId[name];
			};

			// Dedupe nodes and edges. Without this, a measure that is both an `underlying` of
			// one expanded measure AND a `dependant` of another would emit each adjacent edge
			// twice (once per direction of expansion) — the graph visibly accumulated duplicate
			// arrows as the user clicked more nodes.
			const declaredNodes = new Set();
			const declareNode = function (name) {
				const id = ensureId(name);
				if (declaredNodes.has(id)) return;
				declaredNodes.add(id);
				markdown.push(`    ${id}("${name}")`);
				// User-selected nodes get a distinct thick border + highlight fill; "link" nodes
				// (pulled in by the expand-links option) stay plain so the user can tell them
				// apart from their own explicit picks.
				if (userSelected.has(name)) {
					markdown.push(`    style ${id} fill:#ffe082,stroke:#0d6efd,stroke-width:3px`);
				} else {
					markdown.push(`    style ${id} fill:pink`);
				}
			};

			const declaredEdges = new Set();
			const emitEdge = function (from, to) {
				declareNode(from);
				declareNode(to);
				const key = from + "->" + to;
				if (declaredEdges.has(key)) return;
				declaredEdges.add(key);
				markdown.push(`    ${ensureId(from)} --> ${ensureId(to)}`);
			};

			for (const oneMeasure of expanded) {
				const measure = cube.measures[oneMeasure];
				if (!measure) {
					console.warn("Unknown measure: ", oneMeasure);
					continue;
				}

				declareNode(measure.name);

				if (measure.underlying) {
					emitEdge(measure.name, measure.underlying);
				} else if (measure.underlyings) {
					for (const underlying of measure.underlyings) {
						emitEdge(measure.name, underlying);
					}
				}

				const dependants = measureToDependants[oneMeasure];
				if (dependants) {
					for (const dependant of dependants) {
						emitEdge(dependant, measure.name);
					}
				}
			}

			// https://mermaid.js.org/syntax/flowchart.html#interaction
			// Each node becomes clickable and triggers `window.clickAddMeasure(name)`, which adds
			// the measure to the queryModel AND to the in-modal click list (so the graph expands).
			Object.entries(nameToId).forEach(([name, id]) => {
				markdown.push(`    click ${id} href "javascript:clickAddMeasure('${name}');" "Add measure ${name}"`);
			});

			const joinedMarkdown = markdown.join("\n");
			console.log("mermaid markdown: ", joinedMarkdown);
			return joinedMarkdown;
		}

		async function updateGraph(graphDefinition) {
			const id = genSvgId();
			const res = await mermaid.render(id, graphDefinition);
			mermaidString.value = res.svg;
		}

		return { mermaidString, clickedMeasures, expandLinks };
	},
	template: /* HTML */ `
		<!-- Modal -->
		<div class="modal fade" id="measureDag" tabindex="-1" aria-labelledby="measuresDagModalLabel" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered modal-xl">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title" id="measuresDagModalLabel">Measure Info</h5>
						<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
					</div>
					<div class="modal-body">
						<!--
							Toggle: also render every node that lies on a path between any two user-
							selected nodes. Useful to discover the measures that connect otherwise
							disjoint selections, at the cost of pulling a potentially large subgraph.
						-->
						<div class="form-check form-switch mb-2">
							<input class="form-check-input" type="checkbox" role="switch" id="expandLinksBetweenSelected" v-model="expandLinks" />
							<label class="form-check-label text-muted small" for="expandLinksBetweenSelected">
								Add all nodes on paths between selected measures
							</label>
						</div>
						<pre class="mermaid" v-html="mermaidString" />
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
					</div>
				</div>
			</div>
		</div>
	`,
};
