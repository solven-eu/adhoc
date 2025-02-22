import { test, expect } from "@playwright/test";

import fakePlayer from "./fake-player.mjs";

const url = "https://kumite-dev-d2844865d26b.herokuapp.com/";

test("login", async ({ page }) => {
    await page.goto(url);
    await fakePlayer.login(page);
});

test("play-optimization", async ({ page }) => {
    await page.goto(url);
    await fakePlayer.playOptimization(page);
});
