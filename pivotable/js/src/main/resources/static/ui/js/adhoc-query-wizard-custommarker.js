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
	setup() {
		return {};
	},
	template: /* HTML */ `
		<div class="mb-3">
			{{customMarker.path}}
			<label :for="'customMarker_' + customMarker.name" class="form-label">{{customMarker.name}}</label>

			<span v-if="customMarker.possibleValues.length >= 1">
				<!--
					Two optgroups so the dropdown communicates which value is the default without dropping
					the "default-is-also-a-possible" information: the default is promoted to its own labelled
					section first (rendered by browsers as a non-clickable header — the visual separator), and
					the possibleValues array follows in received order. When the default is also a member of
					possibleValues the second occurrence is bolded so the user sees the relationship at a glance.
				-->
				<select
					:id="'customMarker_' + customMarker.name"
					class="form-select"
					:aria-label="customMarker.defaultValue"
					v-model="queryModel.customMarkers[customMarker.path]"
				>
					<optgroup v-if="customMarker.defaultValue" label="Default">
						<option :value="customMarker.defaultValue" selected>{{customMarker.defaultValue}}</option>
					</optgroup>
					<optgroup label="Possible values">
						<option
							:value="possibleValue"
							v-for="possibleValue in customMarker.possibleValues"
							:class="possibleValue === customMarker.defaultValue ? 'fw-bold' : ''"
						>
							{{possibleValue}}
						</option>
					</optgroup>
				</select>
			</span>
			<span v-else>
				<input
					:id="'customMarker_' + customMarker.name"
					type="text"
					class="form-control"
					:placeholder="customMarker.defaultValue"
					v-model="queryModel.customMarkers[customMarker.name]"
				/>
			</span>
		</div>
	`,
};
