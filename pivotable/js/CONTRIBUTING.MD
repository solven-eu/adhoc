
    npm install
    npm ci
    npm audit
    npm run lint

The following commands will reformat properly the whole JS code

    npm run format && mvn spotless:apply

    npm outdated
    npm update
    
    npm install -g npm-check-updates
    ncu -u
    npm install

# Unit tests with ViTests

https://vitest.dev/guide/

    npm run unit_tests

# End-to-end tests with Playwright

https://playwright.dev/docs/intro

    npx playwright test --ui
    npx playwright show-report

## Generate new tests

https://playwright.dev/docs/codegen-intro

    npx playwright codegen http://localhost:8080/
