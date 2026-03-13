import { ref, inject, nextTick, onMounted } from "vue";
import { useUserStore } from "./store-user.js";

/**
 * Floating AI chat assistant for the query builder.
 *
 * Injects `queryModel` from AdhocQuery and applies tool calls (set_measures, set_groupby,
 * clear_query) directly to it, so the wizard checkboxes update in real time.
 *
 * The component is self-hiding: on mount it probes GET /api/v1/cubes/chat/enabled and renders
 * nothing when the backend chat feature is not configured (no Anthropic API key set).
 */
export default {
	props: {
		endpointId: { type: String, required: true },
		cubeId: { type: String, required: true },
	},
	setup(props) {
		const queryModel = inject("queryModel");
		const userStore = useUserStore();

		// Stays false until the probe confirms the backend has chat enabled.
		const isAvailable = ref(false);
		const isOpen = ref(false);
		const isSending = ref(false);

		onMounted(async () => {
			try {
				const response = await userStore.authenticatedFetch("/api/v1/cubes/chat/enabled", {
					method: "GET",
				});
				isAvailable.value = response.ok;
			} catch {
				isAvailable.value = false;
			}
		});
		const userInput = ref("");
		const history = ref([]); // [{ role: "user"|"assistant", content: string }]
		const messagesContainer = ref(null);

		async function scrollToBottom() {
			await nextTick();
			if (messagesContainer.value) {
				messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight;
			}
		}

		async function sendMessage() {
			const message = userInput.value.trim();
			if (!message || isSending.value) return;
			userInput.value = "";

			// Snapshot history before adding the new turn (sent to backend as context)
			const chatHistory = history.value.map((m) => ({ role: m.role, content: m.content }));

			history.value.push({ role: "user", content: message });
			history.value.push({ role: "assistant", content: "" }); // filled by stream
			await scrollToBottom();

			isSending.value = true;

			try {
				const response = await userStore.authenticatedFetch("/api/v1/cubes/chat", {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({
						endpointId: props.endpointId,
						cube: props.cubeId,
						message,
						history: chatHistory,
					}),
				});

				if (!response.ok) {
					throw new Error("Chat request failed (" + response.status + ")");
				}

				const reader = response.body.getReader();
				const decoder = new TextDecoder();
				let buffer = "";

				while (true) {
					const { done, value } = await reader.read();
					if (done) break;

					buffer += decoder.decode(value, { stream: true });
					const lines = buffer.split("\n");
					buffer = lines.pop(); // keep any incomplete trailing line

					for (const line of lines) {
						if (!line.startsWith("data: ")) continue;
						const jsonStr = line.slice(6).trim();
						if (!jsonStr) continue;
						handleEvent(JSON.parse(jsonStr));
					}
					await scrollToBottom();
				}
			} catch (e) {
				history.value[history.value.length - 1].content = "Error: " + e.message;
			} finally {
				isSending.value = false;
				await scrollToBottom();
			}
		}

		function handleEvent(event) {
			if (event.type === "text") {
				history.value[history.value.length - 1].content += event.content;
			} else if (event.type === "tool_use") {
				applyTool(event.name, event.input);
			}
			// "done" and "error" are terminal — handled by stream completion / catch block
		}

		function applyTool(name, input) {
			if (name === "set_measures") {
				Object.keys(queryModel.selectedMeasures).forEach((k) => {
					queryModel.selectedMeasures[k] = false;
				});
				(input.measureNames || []).forEach((m) => {
					queryModel.selectedMeasures[m] = true;
				});
			} else if (name === "set_groupby") {
				Object.keys(queryModel.selectedColumns).forEach((k) => {
					queryModel.selectedColumns[k] = false;
				});
				queryModel.selectedColumnsOrdered.splice(0); // clear in-place to keep reactivity
				(input.columns || []).forEach((c) => {
					queryModel.selectedColumns[c] = true;
					queryModel.selectedColumnsOrdered.push(c);
				});
			} else if (name === "clear_query") {
				Object.keys(queryModel.selectedMeasures).forEach((k) => {
					queryModel.selectedMeasures[k] = false;
				});
				Object.keys(queryModel.selectedColumns).forEach((k) => {
					queryModel.selectedColumns[k] = false;
				});
				queryModel.selectedColumnsOrdered.splice(0);
			}
		}

		return { isOpen, isSending, userInput, history, messagesContainer, sendMessage };
	},
	template: /* HTML */ `
		<!-- Floating toggle button (bottom-right corner) -->
		<button
			class="btn btn-primary rounded-circle position-fixed shadow"
			style="bottom: 1.5rem; right: 1.5rem; width: 3.5rem; height: 3.5rem; font-size: 1.4rem; z-index: 1050;"
			:title="isOpen ? 'Close AI assistant' : 'Open AI query assistant'"
			@click="isOpen = !isOpen"
		>
			<!-- U+1F4AC SPEECH BALLOON emoji -->&#x1F4AC;
		</button>

		<!-- Chat panel -->
		<div
			v-if="isOpen"
			class="card position-fixed shadow-lg"
			style="bottom: 5.5rem; right: 1.5rem; width: 22rem; height: 28rem; z-index: 1049; display: flex; flex-direction: column;"
		>
			<div class="card-header d-flex justify-content-between align-items-center py-2">
				<span class="fw-semibold">AI Query Assistant</span>
				<button class="btn-close" @click="isOpen = false" aria-label="Close"></button>
			</div>

			<!-- Message history -->
			<div class="card-body overflow-auto flex-grow-1 p-2" ref="messagesContainer">
				<p v-if="history.length === 0" class="text-muted small mt-2 text-center">
					Ask me to build a query.<br/>
					e.g. <em>"Show revenue by country"</em>
				</p>
				<div v-for="(msg, i) in history" :key="i" class="mb-2">
					<div :class="msg.role === 'user' ? 'text-end' : 'text-start'">
						<span
							:class="['badge', 'text-wrap', 'text-start', 'lh-base', msg.role === 'user' ? 'bg-primary' : 'bg-secondary']"
							style="max-width: 88%; white-space: pre-wrap; font-weight: normal; font-size: 0.82rem;"
						>{{ msg.content || '…' }}</span>
					</div>
				</div>
			</div>

			<!-- Input bar -->
			<div class="card-footer p-2">
				<div class="input-group input-group-sm">
					<input
						type="text"
						class="form-control"
						v-model="userInput"
						@keydown.enter="sendMessage()"
						:disabled="isSending"
						placeholder="Ask about this cube…"
					/>
					<button
						class="btn btn-primary"
						@click="sendMessage()"
						:disabled="isSending || !userInput.trim()"
					>
						<span v-if="isSending" class="spinner-border spinner-border-sm" role="status"></span>
						<span v-else>Send</span>
					</button>
				</div>
			</div>
		</div>
	`,
};
