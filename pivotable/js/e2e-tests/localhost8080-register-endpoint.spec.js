import { test, expect } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

// Verifies the user can register additional endpoints from the endpoints list and that
// they are persisted client-side. Two registrations exercise the form: one over
// `127.0.0.1:8080`, one over `localhost:8080` — same backend, different host literal,
// proving the URL synthesis treats each as a distinct entry.

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
	const response = await queryPivotable.clear(request, url);
	expect(response.ok()).toBeTruthy();
});

test("register two locally-defined endpoints (127.0.0.1 + localhost)", async ({ page }) => {
	// Wipe any localStorage carried over from a previous run so the registered list
	// starts empty. Done BEFORE first navigation so the SPA boot sees a clean slate.
	await page.goto(url);
	await page.evaluate(() => localStorage.clear());

	await page.reload();
	await queryPivotable.login(page);

	await page.goto(url + "/html/endpoints");
	await expect(page.getByTestId("register-endpoint-card")).toBeVisible();

	// First registration: 127.0.0.1:8080.
	await page.getByTestId("new-endpoint-host").fill("127.0.0.1");
	await page.getByTestId("new-endpoint-port").fill("8080");
	await page.getByTestId("new-endpoint-prefix").fill("");
	await page.getByTestId("new-endpoint-name").fill("loopback");
	await page.getByTestId("new-endpoint-submit").click();

	// The form clears (host empty) once the entry is committed. The new row appears in
	// the list with the user-supplied display name and a "Locally registered" badge.
	await expect(page.getByTestId("new-endpoint-host")).toHaveValue("");
	await expect(page.getByText("Locally registered:")).toHaveCount(1);
	await expect(page.getByText("http://127.0.0.1:8080")).toBeVisible();

	// Second registration: localhost:8080.
	await page.getByTestId("new-endpoint-host").fill("localhost");
	await page.getByTestId("new-endpoint-port").fill("8080");
	await page.getByTestId("new-endpoint-submit").click();

	await expect(page.getByText("http://localhost:8080")).toBeVisible();
	await expect(page.getByText("Locally registered:")).toHaveCount(2);

	// Persistence — reload the page and assert both still show up. This proves the
	// localStorage write/read path works end-to-end (not just the in-memory store).
	// The SESSION cookie carries through `page.reload()`, so no re-login is needed.
	await page.reload();
	await page.goto(url + "/html/endpoints");

	await expect(page.getByText("http://127.0.0.1:8080")).toBeVisible();
	await expect(page.getByText("http://localhost:8080")).toBeVisible();
});
