import { computed, reactive, ref, watch, onMounted } from "vue";

export default {
	// https://vuejs.org/guide/components/registration#local-registration
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
	},
	computed: {},
	setup(props) {
		if (!props.queryModel.options) {
			props.queryModel.options = {};
		}

		return {};
	},
	template: /* HTML */ `
        <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" role="switch" id="explainQuery" v-model="queryModel.options.explain" />
            <label class="form-check-label" for="explainQuery">explain</label>
        </div>
        <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" role="switch" id="debugQuery" v-model="queryModel.options.debug" />
            <label class="form-check-label" for="debugQuery">debug</label>
        </div>
        <div class="form-check form-switch">
            <input class="form-check-input" type="checkbox" role="switch" id="concurrentQuery" v-model="queryModel.options.concurrent" />
            <label class="form-check-label" for="concurrentQuery">concurrent</label>
        </div>
        <div class="form-check form-switch">
            <input
                class="form-check-input"
                type="checkbox"
                role="switch"
                id="UNKNOWN_MEASURES_ARE_EMPTYQuery"
                v-model="queryModel.options.UNKNOWN_MEASURES_ARE_EMPTY"
            />
            <label class="form-check-label" for="UNKNOWN_MEASURES_ARE_EMPTYQuery">UNKNOWN_MEASURES_ARE_EMPTY</label>
        </div>
    `,
};
