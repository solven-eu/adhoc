import {} from "vue";

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
		const toggleTag = function (tag) {
			const tags = props.searchOptions.tags;
			if (tags.includes(tag)) {
				// https://stackoverflow.com/questions/5767325/how-can-i-remove-a-specific-item-from-an-array-in-javascript
				const tagIndex = tags.indexOf(tag);
				tags.splice(tagIndex, 1);

				console.log("Toggling off tag=", tag);
			} else {
				tags.push(tag);

				console.log("Toggling on tag=", tag);
			}
		};

		return { toggleTag };
	},
	// `.prevent` is important so that clicking a click does not select the measure/column toggle, or close the tags dropdown
	// https://stackoverflow.com/questions/45700632/prevent-on-click-on-parent-when-clicking-button-inside-div
	template: /* HTML */ `
        <span type="button" :class="'badge text-bg-' + (searchOptions.tags.includes(tag) ? 'primary' : 'secondary')" @click.prevent="toggleTag(tag)">
            {{tag}}
        </span>
    `,
};
