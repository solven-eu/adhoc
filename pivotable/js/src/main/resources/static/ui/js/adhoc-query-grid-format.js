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
                Minimum fraction digits:
                <input
                    class="form-control mr-sm-2"
                    type="text"
                    placeholder="Min fraction digits"
                    aria-label="Min fraction digits"
                    id="minimumFractionDigits"
                    v-model="formatOptions.minimumFractionDigits"
                />
            </div>
            <div>
                Max fraction digits:
                <input
                    class="form-control mr-sm-2"
                    type="text"
                    placeholder="Max fraction digits"
                    aria-label="Max fraction digits"
                    id="maximumFractionDigits"
                    v-model="formatOptions.maximumFractionDigits"
                />
            </div>
            <div>
                Max significant digits (e.g. if '2', '1234' is shown as '1200'):
                <input
                    class="form-control mr-sm-2"
                    type="text"
                    placeholder="Max significant digits"
                    aria-label="Max significant digits"
                    id="maximumSignificantDigits"
                    v-model="formatOptions.maximumSignificantDigits"
                />
            </div>
            <div>
                roundingPriority
                <input
                    class="form-control mr-sm-2"
                    type="text"
                    placeholder="roundingPriority"
                    aria-label="roundingPriority"
                    id="roundingPriority"
                    v-model="formatOptions.roundingPriority"
                />
            </div>
        </form>
    `,
};
