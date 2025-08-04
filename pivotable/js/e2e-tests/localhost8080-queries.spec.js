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

// We observed some reactivity issues on adding columns
test("queryPivotable.addColumn", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);

    await queryPivotable.addColumn(page, "Shirt Number");

    // TODO Wait for query executed
    // row0
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(0)).toHaveText("0");
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toHaveText("C");
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("0");
    await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("28.00");

    // row1
    await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(0)).toHaveText("1");
    // rowSpan leads to `Position=C` on second row not to exist
    //		await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(1)).toHaveText("C");
    await expect(
        page
            .locator(".slick-row")
            .nth(1)
            .locator(".slick-cell")
            .nth(2 - 1),
    ).toHaveText("2");
    await expect(
        page
            .locator(".slick-row")
            .nth(1)
            .locator(".slick-cell")
            .nth(3 - 1),
    ).toHaveText("19.00");
});

test("queryPivotable.addDependantFromGraph", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);

    await page.getByRole("columnheader", { name: "event_count Header Button" }).locator("span").click();
    await page.getByTitle("DAG about m=event_count").click();
    await page.locator("a").filter({ hasText: "goal_count" }).click();
    await page.getByRole("dialog", { name: "Measure Info" }).getByLabel("Close").click();

    const starCoordinate = false;
    if (starCoordinate) {
        // row0
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("11,270.00");
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("2,256.00");
        // row1
        await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(2)).toHaveText("588.00");
        await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(3)).toHaveText("162.00");
    } else {
        // row0
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("588.00");
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(3)).toHaveText("162.00");
        // row1
        await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(2)).toHaveText("100.00");
        await expect(page.locator(".slick-row").nth(1).locator(".slick-cell").nth(3)).toBeEmpty();
    }
});

test("queryPivotable.browserRefresh", async ({ page }) => {
    await page.goto(url);
    await queryPivotable.queryPivotable(page);

    await page.reload();

    // Check the pivotTable data is available
    const starCoordinate = false;
    if (starCoordinate) {
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("11,270.00");
    } else {
        await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toHaveText("588.00");
    }
});
