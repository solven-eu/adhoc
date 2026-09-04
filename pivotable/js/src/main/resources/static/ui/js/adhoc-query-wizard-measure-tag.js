// @ts-check
import {} from "vue";

import { toggleTag } from "./adhoc-query-wizard-tag-toggle.js";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	props: {
		tag: {
			type: String,
			required: true,
		},
		searchOptions: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const toggle = function (tag) {
			toggleTag(props.searchOptions.tags, tag);
		};

		return { toggle };
	},
	// `.prevent` keeps the click from selecting the enclosing measure/column toggle, or from closing the tags dropdown.
	// `.stop` keeps it from bubbling: in the tags dropdown the badge sits inside a row-wide button which toggles the
	// same tag, and without it a click on the badge would toggle twice and cancel itself out.
	// https://stackoverflow.com/questions/45700632/prevent-on-click-on-parent-when-clicking-button-inside-div
	template: /* HTML */ `
		<span type="button" :class="'badge text-bg-' + (searchOptions.tags.includes(tag) ? 'primary' : 'secondary')" @click.prevent.stop="toggle(tag)">
			{{tag}}
		</span>
	`,
};
