import { expect } from "@playwright/test";

const addColumn = async function (page, column) {
    await page.getByRole("searchbox", { name: "Search" }).dblclick();
    await page.getByRole("searchbox", { name: "Search" }).fill(column);
    await page.getByRole("button", { name: /columns/ }).click();
    await page.getByRole("switch", { name: column }).check();
};

const addMeasure = async function (page, measure) {
    await page.getByRole("searchbox", { name: "Search" }).dblclick();
    await page.getByRole("searchbox", { name: "Search" }).fill(measure);
    // Disable JSON to prevent the depending measures to show up
    await page.getByRole("switch", { name: "JSON" }).uncheck();

    // Add measure=event_count
    //        await expect(page.getByRole("switch", { name: /event_count/ })).toBeVisible({ timeout: 5000 });
    await page.getByRole("button", { name: /measures/ }).click();
    await page.getByRole("switch", { name: measure }).check();
};

// https://stackoverflow.com/questions/33178843/es6-export-default-with-multiple-functions-referring-to-each-other
export default {
    async clear(request, url) {
        const response = await request.post(url + "/api/v1/clear");

        return response;
    },

    async showLoginOptions(page) {
        await page.getByRole("link", { name: /You need to login/ }).click();

        await expect(page.getByRole("link", { name: /github/ })).toBeVisible();
        await expect(page.getByRole("link", { name: /BASIC/ })).toBeVisible();
    },

    async login(page) {
        await this.showLoginOptions(page);
        await page.getByRole("link", { name: /BASIC/ }).click();
        await page.getByRole("button", { name: /Login fakeUser/ }).click();

        await expect(page.getByText(/Welcome Fake User/)).toBeVisible();
    },

    addColumn: addColumn,

    async queryPivotable(page) {
        await page.goto("http://localhost:8080/");
        await page.getByRole("link", { name: /You need to login/ }).click();
        await page.getByRole("link", { name: "BASIC" }).click();
        await page.getByRole("button", { name: "Login fakeUser" }).click();
        await page.getByRole("link", { name: "Browse through endpoints" }).click();
        await page.getByRole("link", { name: /WorldCupPlayers/ }).click();
        await page.getByRole("link", { name: /Query WorldCupPlayers/ }).click();

        await addColumn(page, "Position");

        await addMeasure(page, "event_count");

        const starCoordinate = false;
        if (starCoordinate) {
            // Check some measure value
            await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toContainText("*");
            await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("11,270.00");
        } else {
            await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(1)).toContainText("C");
            await expect(page.locator(".slick-row").nth(0).locator(".slick-cell").nth(2)).toContainText("588.00");
        }
    },
};
