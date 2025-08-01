import { test, expect, request } from "@playwright/test";

import fakePlayer from "./fake-player.mjs";

const url = "http://localhost:8080";

test.beforeAll(async ({ request }) => {
    // Create a new repository
    const response = await fakePlayer.clear(request, url);
    expect(response.ok()).toBeTruthy();
});

// We just check the login page is working OK
test("showLoginOptions", async ({ page }) => {
    await page.goto(url);
    await fakePlayer.showLoginOptions(page);
});

test("login", async ({ page }) => {
    await page.goto(url);
    await fakePlayer.login(page);
});

test("play-1v1", async ({ page }) => {
    await page.goto(url);
//    await fakePlayer.login(page);
//    await fakePlayer.playMultiplayers(page, /Tic-Tac-Toe/);


  await page.goto('http://localhost:8080/');
  await page.getByRole('link', { name: ' You need to login' }).click();
  await page.getByRole('link', { name: 'BASIC' }).click();
  await page.getByRole('button', { name: 'Login fakeUser' }).click();
  await page.getByRole('link', { name: 'Browse through endpoints' }).click();
  await page.getByRole('link', { name: ' WorldCupPlayers' }).click();
  await page.getByRole('link', { name: ' Query WorldCupPlayers' }).click();
  await page.getByRole('button', { name: 'columns   0' }).click();
  await page.getByRole('searchbox', { name: 'Search' }).click();
  await page.getByRole('searchbox', { name: 'Search' }).fill('Posi');
  await page.getByRole('switch', { name: 'Position' }).check();
  await page.getByRole('button', { name: '1 columns   1' }).click();
  await page.getByRole('button', { name: '3 measures   0' }).click();
  await page.getByRole('switch', { name: 'JSON' }).uncheck();
  await page.getByRole('searchbox', { name: 'Search' }).dblclick();
  await page.getByRole('searchbox', { name: 'Search' }).fill('');
  await page.getByRole('switch', { name: 'event_count ?' }).check();
  await page.getByRole('gridcell', { name: '11,270.00' }).click();
});
