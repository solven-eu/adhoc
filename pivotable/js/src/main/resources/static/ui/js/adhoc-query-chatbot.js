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
				// authenticatedFetch prefixes "/api/v1" automatically — pass only the path under that.
				// The endpoint is ALWAYS mounted now (regardless of whether an Anthropic API key is configured) and
				// always returns 200 with a JSON body `{enabled: boolean, reason?: string, retryAfterSeconds?: number}`.
				// The SPA hides the chatbot icon when `enabled === false` and logs the reason so a configuration miss
				// is debuggable from the browser console.
				const response = await userStore.authenticatedFetch("/cubes/chat/enabled", {
					method: "GET",
				});
				if (response.ok) {
					const data = await response.json();
					isAvailable.value = data && data.enabled === true;
					if (!isAvailable.value) {
						console.info("Chat assistant hidden:", data && data.reason ? data.reason : "(no reason)", data);
					}
				} else {
					isAvailable.value = false;
					console.info("Chat assistant hidden: /api/v1/cubes/chat/enabled returned", response.status);
				}
			} catch (e) {
				isAvailable.value = false;
				console.warn("Chat assistant hidden: probe to /api/v1/cubes/chat/enabled threw", e);
			}
		});
		const userInput = ref("");
		const history = ref([]); // [{ role: "user"|"assistant", content: string }]
		const messagesContainer = ref(null);
		// Template ref to the text input so we can refocus after each send. The input gets `:disabled`'d while
		// isSending is true, which by default makes the browser drop keyboard focus — typing another message would
		// otherwise require an explicit click into the field every turn.
		const userInputEl = ref(null);

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
				// authenticatedFetch prefixes "/api/v1" automatically.
				const response = await userStore.authenticatedFetch("/cubes/chat", {
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
				let sawDone = false;

				while (true) {
					const { done, value } = await reader.read();
					if (done) break;

					buffer += decoder.decode(value, { stream: true });
					const lines = buffer.split("\n");
					buffer = lines.pop(); // keep any incomplete trailing line

					for (const line of lines) {
						// SSE spec allows both `data:value` and `data: value` (the leading space is optional).
						// Spring's WebFlux SSE codec emits `data:` with no space; the previous parser only
						// accepted the `data: ` variant and silently dropped every event.
						if (!line.startsWith("data:")) continue;
						const jsonStr = line.slice(5).trim();
						if (!jsonStr) continue;
						const event = JSON.parse(jsonStr);
						handleEvent(event);
						if (event.type === "done" || event.type === "error") {
							sawDone = true;
						}
					}
					await scrollToBottom();

					// Defensive: break the loop once the terminal event arrives, in case the upstream
					// (Anthropic / our mock / a proxy) keeps the connection open after `message_stop`.
					// Without this, reader.read() blocks forever and the Send button stays greyed.
					// We fire-and-forget the cancel (no await) — awaiting it can hang on mocked responses.
					if (sawDone) {
						reader.cancel().catch(() => {});
						break;
					}
				}
			} catch (e) {
				const lastErr = history.value.length - 1;
				history.value.splice(lastErr, 1, { ...history.value[lastErr], content: "Error: " + e.message });
			} finally {
				isSending.value = false;
				await scrollToBottom();
				// Restore focus to the input so the user can type the next message without re-clicking.
				// nextTick lets Vue first re-render `:disabled="isSending"` back to enabled — focus() on a
				// disabled element is a no-op.
				await nextTick();
				if (userInputEl.value) {
					userInputEl.value.focus();
				}
			}
		}

		function handleEvent(event) {
			if (event.type === "text") {
				// Vue 3 sometimes misses an in-place `obj.content += chunk` mutation on an element of a `ref<Array>`
				// when the underlying object was added via push of a plain literal. Replace the array element wholesale
				// to guarantee the template re-renders for each streamed chunk.
				const last = history.value.length - 1;
				history.value.splice(last, 1, {
					...history.value[last],
					content: history.value[last].content + event.content,
				});
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

		return { isAvailable, isOpen, isSending, userInput, history, messagesContainer, userInputEl, sendMessage };
	},
	template: /* HTML */ `
		<!-- Floating toggle button (bottom-right corner). Self-hides when the probe
             /api/v1/cubes/chat/enabled did not return 2xx (typically: no Anthropic API
             key set, or MCP server down). -->
		<button
			v-if="isAvailable"
			class="btn btn-primary rounded-circle position-fixed shadow"
			style="bottom: 1.5rem; right: 1.5rem; width: 3.5rem; height: 3.5rem; font-size: 1.4rem; z-index: 1050;"
			:title="isOpen ? 'Close AI assistant' : 'Open AI query assistant'"
			@click="isOpen = !isOpen"
		>
			<!-- U+1F4AC SPEECH BALLOON emoji -->&#x1F4AC;
		</button>

		<!-- Chat panel -->
		<div
			v-if="isAvailable && isOpen"
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
					Ask me to build a query.<br />
					e.g. <em>"Show revenue by country"</em>
				</p>
				<div v-for="(msg, i) in history" :key="i" class="mb-2">
					<div :class="msg.role === 'user' ? 'text-end' : 'text-start'">
						<span
							:class="['badge', 'text-wrap', 'text-start', 'lh-base', msg.role === 'user' ? 'bg-primary' : 'bg-secondary']"
							style="max-width: 88%; white-space: pre-wrap; font-weight: normal; font-size: 0.82rem;"
							>{{ msg.content || '…' }}</span
						>
					</div>
				</div>
			</div>

			<!-- Input bar -->
			<div class="card-footer p-2">
				<div class="input-group input-group-sm">
					<input
						type="text"
						class="form-control"
						ref="userInputEl"
						v-model="userInput"
						@keydown.enter="sendMessage()"
						:disabled="isSending"
						maxlength="500"
						placeholder="Ask about this cube…"
					/>
					<button class="btn btn-primary" @click="sendMessage()" :disabled="isSending || !userInput.trim()">
						<span v-if="isSending" class="spinner-border spinner-border-sm" role="status"></span>
						<span v-else>Send</span>
					</button>
				</div>
			</div>
		</div>
	`,
};
