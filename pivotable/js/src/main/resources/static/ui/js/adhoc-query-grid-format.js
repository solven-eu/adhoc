import { ref, watch, onMounted, reactive } from "vue";

export default {
	components: {},
	// https://vuejs.org/guide/components/props.html
	props: {
		formatOptions: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		return {};
	},
	template: /* HTML */ `
        <form>
            <div>
                Locale:
                <input class="form-control mr-sm-2" type="text" placeholder="Locale" aria-label="Locale" id="locale" v-model="formatOptions.locale" />
            </div>
            <div>
                Currency:
                <input class="form-control mr-sm-2" type="text" placeholder="Currency" aria-label="Currency" id="currency" v-model="formatOptions.measureCcy" />
            </div>
            <div>
                Max Digits:
                <input
                    class="form-control mr-sm-2"
                    type="text"
                    placeholder="Max digits"
                    aria-label="Max digits"
                    id="maxDigits"
                    v-model="formatOptions.measureMaxDigits"
                />
            </div>
        </form>
    `,
};
