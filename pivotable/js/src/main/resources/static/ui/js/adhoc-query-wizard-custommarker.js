import {} from "vue";

export default {
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
		customMarker: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		return {};
	},
	template: /* HTML */ `
        <div class="mb-3">
            <label :for="'customMarker_' + customMarker.name" class="form-label">{{customMarker.name}}</label>

			<span :id="'customMarker_' + customMarker.name">
	            <span v-if="customMarker.possibleValues.length >= 1">
	                <select class="form-select" :aria-label="customMarker.defaultValue" v-model="queryModel.customMarkers[customMarker.name]">
	                    <option :value="customMarker.defaultValue" selected v-if="customMarker.defaultValue">{{customMarker.defaultValue}}</option>
	                    <option :value="possibleValue" v-for="possibleValue in customMarker.possibleValues">{{possibleValue}}</option>
	                </select>
	            </span>
	            <span v-else>
	                <input
	                    type="text"
	                    class="form-control"
	                    :placeholder="customMarker.defaultValue"
	                    v-model="queryModel.customMarkers[customMarker.name]"
	                />
	            </span>
			</span>
        </div>
    `,
};
