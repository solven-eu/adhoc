import { test, expect, request } from "@playwright/test";

import queryPivotable from "./query-pivotable.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
    // Create a new repository
    const response = await queryPivotable.clear(request, url);
    expect(response.ok()).toBeTruthy();
});

// We just check the login page is working OK
test("showLoginOptions", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.showLoginOptions(page);
});

test("login", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.login(page);
});

test("queryPivotable", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);
});

test("queryPivotable.addDependantFromGraph", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);

    await page.getByRole("columnheader", { name: "event_count Header Button" }).locator("span").click();
    await page.getByTitle("DAG about m=event_count").click();
    await page.locator("a").filter({ hasText: "goal_count" }).click();
    await page.getByRole("dialog", { name: "Measure Info" }).getByLabel("Close").click();
    // row0
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("11,270.00");
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toContainText("2,256.00");
    // row1
    await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(2)).toContainText("588.00");
    await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(3)).toContainText("162.00");
});

test("queryPivotable.browserRefresh", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);

    await page.reload();

    // Check the pivotTable data is available
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("11,270.00");
});
