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
		const expanded = [];

		const cube = inject("cube");
		const queryModel = inject("queryModel");

		// expanded.push(props.measure.name);

		// https://github.com/hojas/vue-mermaid-render/blob/main/packages/vue-mermaid-render/src/components/VueMermaidRender.vue
		const mermaidString = ref("");

		mermaid.initialize({
			startOnLoad: false,
			securityLevel: "loose",
			// From 0 to 5 (0 is ALL, 5 is none)
			logLevel: 5,
		});

		// TODO For any reason, this is not called-back
		window.clickAddMeasure = (measure) => {
			console.info("Clicked m=", measure);
			queryModel.selectedMeasures[measure] = true;
		};

		// Revers the `measure->underlying` mapping, in order to show measures depending on selected emeasure
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

		watch(
			() => props.measuresDagModel,
			(newValue, oldValue) => {
				expanded.splice(0, expanded.length);

				// Show the main measure
				if (props.measuresDagModel.main) {
					expanded.push(props.measuresDagModel.main);
				}
				// And the set of additional measures
				for (const m of props.measuresDagModel.highlight) {
					expanded.push(m);
				}

				if (expanded.length == 0) {
					// This should never happen. We cover the case just in case.
					updateGraph("flowchart TD\r\n    empty");
				} else {
					if (Object.keys(measureToDependants).length == 0) {
						// Lazy-initialisation of measureToDependants
						fillMeasureToDependants();
					}

					updateGraph(computeMarkdown());
				}
			},
			// Deep seems needed to handle changes in `highlight` array
			{ deep: true },
		);

		/**
		 * generate svg id
		 */
		function genSvgId() {
			const max = 1000000;
			return `mermaid-svg-${genId(max)}${genId(max)}`;

			function genId(max) {
				return ~~(Math.random() * max);
			}
		}

		function computeMarkdown() {
			let markdown = [];

			// `LR` will have better rendering on measure with many dependents, as long measure will not prevent each measure to have a fixed height
			markdown.push("flowchart LR");

			// From measure name to some mermaid friendly id
			const nameToId = {};
			// const idToName = {};

			// Compute a uniqueId per measure
			// Necessary as Mermaid required an id with simple characters
			// https://github.com/mermaid-js/mermaid/issues/2388
			// https://github.com/mermaid-js/mermaid/issues/4182
			const ensureId = function (name) {
				if (nameToId[name] === undefined) {
					const measureId = "id" + Object.keys(nameToId).length;
					nameToId[name] = measureId;
					// idToName[measureId] = name;
					console.log(`${name} has id=${measureId}`);
				}
				return nameToId[name];
			};

			for (let oneMeasure of expanded) {
				const measure = cube.measures[oneMeasure];

				if (!measure) {
					console.warn("Unknown measure: ", oneMeasure);
					return;
				}

				markdown.push(`    ${ensureId(measure.name)}("${measure.name}")`);
				// https://mermaid.js.org/syntax/flowchart.html#styling-a-node
				markdown.push(`    style ${ensureId(measure.name)} fill:pink`);

				if (measure.underlying) {
					markdown.push(`    ${ensureId(measure.name)} --> ${ensureId(measure.underlying)}("${measure.underlying}")`);
				} else if (measure.underlyings) {
					for (const underlying of measure.underlyings) {
						markdown.push(`    ${ensureId(measure.name)} --> ${ensureId(underlying)}("${underlying}")`);
					}
				}

				const dependants = measureToDependants[oneMeasure];
				if (dependants) {
					for (const dependant of dependants) {
						markdown.push(`    ${ensureId(dependant)}("${dependant}") --> ${ensureId(measure.name)}`);
					}
				}
				// else: this is a top measure, without any dependant
			}

			// https://mermaid.js.org/syntax/flowchart.html#interaction
			// https://github.com/mermaid-js/mermaid/issues/731
			Object.entries(nameToId).forEach(([name, id]) => {
				// https://stackoverflow.com/questions/72922602/event-on-node-not-calling-the-function-when-using-the-render-function
				// markdown.push(`    click ${id} call clickAddMeasure() "Add measure ${name}"`);
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

		return { computeMarkdown, mermaidString };
	},
	template: /* HTML */ `
        <!-- Modal -->
        <div class="modal fade" id="measureDag" tabindex="-1" aria-labelledby="measuresDagModalLabel" aria-hidden="true">
            <div class="modal-dialog  modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="measuresDagModalLabel">Measure Info</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
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
