import { expect } from "@playwright/test";

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

    async playOptimization(page) {
        await page.getByRole("link", { name: /Games/ }).click();

        await page.getByRole("link", { name: /Travelling Salesman Problem/ }).click();
        await page.getByRole("link", { name: /Create your own contest/ }).click();

        // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Math/random
        const contestName = String(Math.floor(Math.random() * 1024));
        await page.locator("#contestName").fill(contestName);
        await page.getByRole("button", { name: "Submit" }).click();

        // await page.getByRole('link', { name: /Auto-generated/ }).first().click();
        await page
            .getByRole("link", { name: new RegExp(`${contestName}`, "gi") })
            .first()
            .click();

        await page.getByRole("button", { name: "Preview the board" }).click();
        await page.getByRole("button", { name: "Join contest as player" }).click();
        await page.getByRole("button", { name: /Play a move/ }).click();
        await page.getByRole("button", { name: "Load some available moves" }).click();
        await page.getByRole("button", { name: "Prefill with an example move" }).click();
        await page.getByText("greedy").click();
        await page.getByRole("button", { name: "Submit" }).click();
        // TODO Find a way to properly test the leaderboard
        await expect(page.getByText("11111111-1111-1111-1111-111111111111")).toBeVisible();
        await expect(page.getByText(/has score/)).toBeVisible();
    },

    async playMultiplayers(page, gameRegex) {
        await page.getByRole("link", { name: /Games/ }).click();

        await page.getByRole("link", { name: gameRegex }).click();
        await page.getByRole("link", { name: /Create your own contest/ }).click();

        // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Math/random
        const contestName = String(Math.floor(Math.random() * 1024));
        await page.locator("#contestName").fill(contestName);
        await page.getByRole("button", { name: "Submit" }).click();

        // await page.getByRole('link', { name: /Auto-generated/ }).first().click();
        await page
            .getByRole("link", { name: new RegExp(`${contestName}`, "gi") })
            .first()
            .click();

        await page.getByRole("button", { name: "Preview the board" }).click();
        await page.getByRole("button", { name: "Join contest as player" }).click();
        await page.getByRole("button", { name: "Load some available moves" }).click();
        await page.getByRole("button", { name: "Prefill with an example move" }).click();
        await page.getByTestId("move_0").click();
        await page.getByRole("button", { name: "Submit" }).click();
        // TODO Find a way to properly test the leaderboard
        await expect(page.getByText("11111111-1111-1111-1111-111111111111")).toBeVisible();
        await expect(page.getByText(/has score/)).toBeVisible();
    },
};
