import { mapState } from "pinia";
import { useAdhocStore } from "./store.js";

export default {
	computed: {
		...mapState(useAdhocStore, ["account", "nbAccountFetching"]),
	},
	setup() {
		return {};
	},
	template: /* HTML */ `
        <h1>Kumite</h1>
        This is a plateform for bots/algorithms contests. Links
        <ul>
            <li>
                <a href="https://github.com/solven-eu/kumite/" target="_blank">Github project</a>
            </li>

            <li>
                <a href="./swagger-ui.html" target="_blank">OpenAPI</a>
            </li>
        </ul>

        <a href="https://www.solven.eu/kumite/lexicon/" target="_blank">Lexicon</a>
    `,
};
