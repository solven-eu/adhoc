import { inject } from "vue";

export default {
	name: "AdhocQueryWizardFilter",
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		filter: {
			type: Object,
			required: true,
		},
		path: {
			type: Array,
			default: [],
		},
	},
	computed: {},
	// emits: ['removeFilter'],
	setup(props) {
		const childrenPath = function (subComponent) {
			let newArray = props.path.slice();

			newArray.push(subComponent);

			return newArray;
		};

		const queryModel = inject("queryModel");

		const removeFilter = function () {
			// ctx.emit('removeFilter', {path: props.path});

			// Start drilling from the root
			let filterSubObject = queryModel.filter;

			const pathLength = props.path.length;

			if (pathLength == 0) {
				console.log("Clearing the whole filter");
				queryModel.filter = {};
			} else {
				for (let pathIndex = 0; pathIndex < pathLength; pathIndex++) {
					if (!filterSubObject) {
						console.log("Drilled filter is empty");
						break;
					} else if (filterSubObject.type === "and" || filterSubObject.type === "or") {
						filterSubObject = filterSubObject.filters;
					}

					const pathComponent = props.path[pathIndex];

					if (pathIndex == pathLength - 1) {
						console.log("Removing", pathComponent, "from", filterSubObject);
						// delete filterSubObject[pathComponent];
						filterSubObject.splice(pathComponent, 1);
					} else {
						const drilledFilterSubObject = filterSubObject[pathComponent];
						console.log("Drilling for filter removal. ", pathComponent, filterSubObject, drilledFilterSubObject);
						filterSubObject = drilledFilterSubObject;
					}
				}
			}
		};

		return { childrenPath, removeFilter };
	},
	template: /* HTML */ `
        <div v-if="!filter">NULL?</div>
        <div v-else-if="filter.type === 'and'">
            AND<button type="button" class="btn"><i class="bi bi-x-circle" @click="removeFilter"></i></button>
            <ul>
                <li v-for="(operand, index) in filter.filters"><AdhocQueryWizardFilter :filter="operand" :path="childrenPath(index)" /></li>
            </ul>
        </div>
        <div v-else-if="filter.type === 'or'">
            OR<button type="button" class="btn"><i class="bi bi-x-circle" @click="removeFilter"></i></button>
            <ul>
                <li v-for="(operand, index) in filter.filters"><AdhocQueryWizardFilter :filter="operand" :path="childrenPath(index)" /></li>
            </ul>
        </div>
        <span v-else-if="filter.type==='column'">
            {{filter.column}}={{filter.valueMatcher}} <button type="button" class="btn"><i class="bi bi-x-circle" @click="removeFilter"></i></button>
        </span>
        <div v-else>{{filter}}</div>
    `,
};
