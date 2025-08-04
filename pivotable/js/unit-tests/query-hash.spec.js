import { expect, test } from "vitest";

//import lodashEs from "https://cdn.jsdelivr.net/npm/lodash-es@4.17.21/+esm";

import queryHelper from "@/js/adhoc-query-helper.js";

// https://vitest.dev/api/expect.html
test("loadQueryModelFromHash - undefined hash", () => {
    const reloadedQueryModel = queryHelper.makeQueryModel();
    queryHelper.hashToQueryModel(undefined, reloadedQueryModel);

    expect(reloadedQueryModel.selectedColumns).toEqual({});
    expect(reloadedQueryModel.selectedColumnsOrdered).toEqual([]);
});

test("loadQueryModelFromHash - from 2 columns", () => {
    const originalQueryModel = queryHelper.makeQueryModel();
    originalQueryModel.selectedColumns.c1 = true;
    originalQueryModel.selectedColumns.c2 = false;
    originalQueryModel.selectedColumnsOrdered.push("c1");

    const queryModel = JSON.parse(JSON.stringify(originalQueryModel));
    const newHash = queryHelper.queryModelToHash(undefined, queryModel);

    if (!newHash.startsWith("#")) {
        fail("Should starts with '#'");
    }

    expect(newHash).toEqual(
        "#" + encodeURIComponent(JSON.stringify({ query: { columns: ["c1"], withStarColumns: {}, measures: [], filter: {}, customMarkers: {}, options: [] } })),
    );
    ("#%7B%22query%22%3A%7B%22columns%22%3A%5B%5D%2C%22withStarColumns%22%3A%7B%7D%2C%22measures%22%3A%5B%5D%2C%22filter%22%3A%7B%7D%2C%22customMarkers%22%3A%7B%7D%2C%22options%22%3A%5B%5D%7D%7D");

    const reloadedQueryModel = queryHelper.makeQueryModel();
    queryHelper.hashToQueryModel(decodeURIComponent(newHash), reloadedQueryModel);

    expect(reloadedQueryModel.selectedColumns).toEqual({ c1: true });
    expect(reloadedQueryModel.selectedColumnsOrdered).toEqual(["c1"]);
});
