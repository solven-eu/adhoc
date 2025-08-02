// @ts-check

import { defineConfig, devices } from "@playwright/test";

/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
// require('dotenv').config({ path: path.resolve(__dirname, '.env') });

// `../../` as we're in `./pivotable/js`
const commandSpringBoot = `(mvn -f ../server/pom.xml -Pfast spring-boot:run -Dspring-boot.run.profiles=${process.env.SPRING_ACTIVE_PROFILES};)`;
console.log("commandSpringBoot", commandSpringBoot);

/**
 * @see https://playwright.dev/docs/test-configuration
 */
const config = defineConfig({
    testDir: "./e2e-tests",
    /* Run tests in files in parallel */
    fullyParallel: true,
    /* Fail the build on CI if you accidentally left test.only in the source code. */
    forbidOnly: !!process.env.CI,
    /* Retry on CI only */
    retries: process.env.CI ? 2 : 0,
    /* Opt out of parallel tests on CI. */
    workers: process.env.CI ? 1 : undefined,
    /* Reporter to use. See https://playwright.dev/docs/test-reporters */
    reporter: "html",
    /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
    use: {
        /* Base URL to use in actions like `await page.goto('/')`. */
        // baseURL: 'http://127.0.0.1:3000',

        /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
        trace: "on-first-retry",

        // https://github.com/microsoft/playwright/issues/14854
        screenshot: "only-on-failure",
    },

    // https://playwright.dev/docs/test-timeouts
    timeout: 15000,
    expect: { timeout: 2000 },

    /* Configure projects for major browsers */
    projects: [
        {
            name: "chromium",
            use: { ...devices["Desktop Chrome"] },

            // https://martinmcgee.dev/posts/how-to-ignore-cors-playwright/
            // https://playwright.dev/docs/api/class-testoptions#test-options-bypass-csp
            bypassCSP: true, // add this to disable cors
            launchOptions: {
                args: ["--disable-web-security"], // add this to disable cors
            },
        },

        {
            name: "firefox",
            use: { ...devices["Desktop Firefox"] },
        },

        {
            name: "webkit",
            use: { ...devices["Desktop Safari"] },
        },

        /* Test against mobile viewports. */
        // {
        //   name: 'Mobile Chrome',
        //   use: { ...devices['Pixel 5'] },
        // },
        // {
        //   name: 'Mobile Safari',
        //   use: { ...devices['iPhone 12'] },
        // },

        /* Test against branded browsers. */
        // {
        //   name: 'Microsoft Edge',
        //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
        // },
        // {
        //   name: 'Google Chrome',
        //   use: { ...devices['Desktop Chrome'], channel: 'chrome' },
        // },
    ],

    /* Run your local dev server before starting the tests */
    webServer: {
        //   command: 'npm run start',
        // `mvn install` to ensure `js` is fresh (especially important in local dev)
        // `cd ../server` to relocate in the backend folder
        // `mvn spring-boot:run` to effectively starts a server
        command: commandSpringBoot,
        //   url: 'http://127.0.0.1:3000',
        url: "http://127.0.0.1:8080",
        reuseExistingServer: !process.env.CI,
    },
});

// https://github.com/microsoft/playwright/issues/12138
export default config;
