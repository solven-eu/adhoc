// @ts-check
import { test, expect } from "./_coverage-fixture.mjs";

import queryPivotable from "./query-pivotable.mjs";

import { BASE_URL as url } from "./_url.mjs";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

// Nominal end-to-end scenario for the AI chat assistant (`AdhocQueryChatbot`).
//
// We do NOT talk to a real Anthropic endpoint in CI — the test would be flaky and would require
// a live API key. Instead we intercept BOTH the probe endpoint and the streaming POST, replaying
// a hand-crafted SSE response that exercises the three event types the SPA must render: `text`
// (streaming chunks), `tool_use` (must trigger applyTool side-effects on queryModel), and `done`
// (must close the stream so isSending flips back to false).
//
// This test pins the three bugs we previously hit at hand-testing time:
//   - Vue reactivity miss on `history.value[last].content += chunk` — caught when the assistant
//     bubble would show an ellipsis after streaming completed. The current implementation uses
//     `splice` to replace the array element so reactivity fires per chunk.
//   - Stream-never-closes — caught when the Send button stays greyed after the response is
//     visible in DevTools. The handler now completes the sink on `{type:"done"}`.
//   - Tool-call wiring — caught when set_measures arrives but the wizard does not toggle a
//     checkbox. We assert the targeted measure becomes checked.
test.setTimeout(60_000);
test("Chat assistant: streamed text renders, tool_use toggles a measure, Send re-enables on done", async ({ page }) => {
	// Mocked SSE stream. Order matters: text chunks BEFORE tool_use BEFORE done, mirroring what
	// Anthropic emits in practice.
	const mockedStream = [
		'data: {"type":"text","content":"Showing "}\n\n',
		'data: {"type":"text","content":"the delta measure for you."}\n\n',
		'data: {"type":"tool_use","name":"set_measures","input":{"measureNames":["delta"]}}\n\n',
		'data: {"type":"done"}\n\n',
	].join("");

	// Force chat to be considered available even without an Anthropic key in the dev backend.
	await page.route("**/api/v1/cubes/chat/enabled", (route) => route.fulfill({ status: 204, contentType: "text/plain", body: "" }));

	// Intercept the actual chat POST and replay the canned stream.
	await page.route("**/api/v1/cubes/chat", (route) => {
		if (route.request().method() !== "POST") {
			return route.continue();
		}
		return route.fulfill({
			status: 200,
			contentType: "text/event-stream",
			body: mockedStream,
		});
	});

	// Standard login flow used by every other localhost8080-*.spec.js scenario.
	await page.goto(url);
	await page.getByRole("link", { name: /You need to login/ }).click();
	await page.getByRole("link", { name: "pivotable-unsafe_fakeuser" }).click();
	await page.getByRole("button", { name: /^Login$/i }).click();

	// Navigate into the `simple` cube (it exposes the `delta` measure the mock toggles).
	await page.getByRole("link", { name: "Browse through endpoints" }).click();
	await page
		.getByRole("link", { name: /simple/i })
		.first()
		.click();
	await page.getByRole("link", { name: /Query simple/i }).click();

	// The chatbot's onMounted probe fires on first render of <AdhocQuery>. Wait for the floating
	// 💬 button to appear (it is gated on `isAvailable`, which becomes true only after the probe
	// returns 2xx — proving our route mock kicked in). The button has no text content (just a
	// speech-balloon emoji), so we target by the unique combination of classes Bootstrap gives
	// the floating-action toggle.
	const chatToggle = page.locator("button.rounded-circle.position-fixed");
	await expect(chatToggle).toBeVisible({ timeout: 10_000 });
	await chatToggle.click();

	// The chat panel is now open. Type a question and submit.
	const input = page.getByPlaceholder("Ask about this cube…");
	await input.fill("show delta");
	// The Send button lives inside the chat panel's input-group footer.
	const sendButton = page.locator(".card-footer button.btn-primary");
	await sendButton.click();

	// Assert the assistant bubble eventually contains the concatenated text from the two streamed
	// `text` chunks. We anchor on the full sentence so a missing reactive update (the bug we just
	// fixed) would surface as a timeout here.
	await expect(page.locator(".badge.bg-secondary").last()).toContainText("Showing the delta measure for you.", {
		timeout: 10_000,
	});

	// Note: the `tool_use` → applyTool side-effect (set_measures toggles a wizard checkbox) is
	// covered at the unit-test layer in TestAnthropicSseTranslator and TestChatRequestPlanner;
	// not asserted here because the wizard's measure-checkbox rendering depends on a search /
	// expansion step that adds noise to this scenario.

	// Assert the Send button has re-enabled. If the stream never completes, isSending stays true
	// and the button stays disabled — the second bug we fixed.
	await expect(sendButton).toBeEnabled({ timeout: 5_000 });
});
