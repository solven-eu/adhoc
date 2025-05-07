import {} from "vue";

import { markMatchingWizard } from "./adhoc-query-wizard-search-helpers.js";

export default {
	props: {
		measure: {
			type: Object,
			required: true,
		},
		showDetails: {
			type: Boolean,
			default: true,
		},
		searchOptions: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const mark = function (text) {
			return markMatchingWizard(props.searchOptions, text);
		};

		const toggleTag = function (tag) {
			console.log("Toggling", tag);

			const tags = props.searchOptions.tags;
			if (tags.includes(tag)) {
				// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
				const tagIndex = tags.indexOf(tag);
				tags.splice(tagIndex, 1);
			} else {
				tags.push(tag);
			}
		};

		const filteredEntry = function (measure) {
			const filteredCopy = Object.assign({}, measure);

			// Name is shown as header: no need to show it again in details
			delete filteredCopy.name;
			// Unclear why we receive a `key` which duplicate the `name`
			delete filteredCopy.key;
			// tags are shown as badges
			delete filteredCopy.tags;

			return filteredCopy;
		};

		return { mark, toggleTag, filteredEntry };
	},
	template: /* HTML */ `
        <span v-html="mark(measure.name)" />
        <span
            v-for="tag in measure.tags"
            :class="'badge text-bg-' + (searchOptions.tags.includes(tag) ? 'primary' : 'secondary')"
            @click.prevent="toggleTag(tag)"
        >
            {{tag}}
        </span>
        <div v-if="showDetails" class="text-muted">
            <span v-if="measure.type == '.Aggregator'">
                <small v-html="mark(measure.aggregationKey + '(' + measure.columnName + ')')" />
            </span>
            <span v-else-if="measure.type == '.Combinator'">
                <small v-html="mark(measure.combinationKey + '(' + measure.underlyings.join(', ') + ')')" />
            </span>
            <span v-else-if="measure.type == '.Dispatchor'">
                <small v-html="'dispatching by ' + mark(measure.decompositionKey + '(' + measure.underlyings.join(', ') + ')')" />
            </span>
            <span v-else-if="measure.type == '.Filtrator'">
                <small v-html="'filtering ' + mark(measure.filter + '(' + measure.underlying + ')')" />
            </span>
            <small v-else>
				<ul>
					<li v-for="(value, key) in filteredEntry(measure)">
						{{key}}: <span v-html="mark(value)"></span>
					</li>
				</ul>
            </small>
        </div>
    `,
};
