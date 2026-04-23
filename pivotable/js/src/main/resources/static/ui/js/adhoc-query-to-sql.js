// Minimal queryModel -> SQL converter. Like the sibling MDX converter (adhoc-query-to-mdx.js)
// this is intended for side-by-side comparison / copy-paste into an external SQL client. The
// produced SQL is NOT the query Adhoc actually runs — Adhoc evaluates the JSON queryModel via
// its own engine, and many Adhoc concepts (post-processors, custom markers, column filters on
// measures, …) have no direct SQL equivalent. Keep this module dependency-free so the JS bundle
// stays small and the converter can evolve without a backend deploy.
//
// SQL primer (for future edits):
//   SELECT <group-by columns>, <measures>
//   FROM   "<cube>"
//   WHERE  <row predicate>
//   GROUP BY <columns>
//
// Conventions used here:
//   - Identifiers are delimited with ANSI double-quotes (`"col"`); embedded `"` is doubled.
//   - String literals use single-quotes; embedded `'` is doubled.
//   - Numbers and booleans are emitted as bare tokens.
//   - `null` predicates become `IS NULL` (though the current UI rarely produces them).
//   - Measure names are projected as plain identifiers; Adhoc measures are opaque to SQL so
//     we do NOT guess an aggregation function — the user is expected to wrap the identifier
//     with the appropriate SQL aggregate (SUM, AVG, …) for their real schema.
//   - withStarColumns (grandTotal) => the column is wrapped in ROLLUP() inside GROUP BY, so
//     the result set contains both the per-value rows AND the aggregated grand-total row.
//
// Out of scope (falls back to a `/* … */` comment in the output, like the MDX converter):
//   - LikeMatcher / InMatcher / NotMatcher — could be added with LIKE / IN / <> later.
//   - customMarkers (no SQL equivalent).

// Escape `"` inside an identifier by doubling it.
const escId = function (name) {
	return String(name).replace(/"/g, '""');
};

// ANSI-quote an identifier: `col` -> `"col"`, `a"b` -> `"a""b"`.
const quoteId = function (name) {
	return '"' + escId(name) + '"';
};

// Format a primitive value as a SQL literal. Numbers/booleans are bare; strings are
// single-quoted with embedded quotes doubled; null/undefined becomes NULL.
const literal = function (v) {
	if (v === null || v === undefined) return "NULL";
	if (typeof v === "number" || typeof v === "boolean") return String(v);
	return "'" + String(v).replace(/'/g, "''") + "'";
};

// Extract the primitive value carried by a ColumnFilter's `valueMatcher`. Same shapes as the
// MDX converter accepts — raw primitive or `{ type: 'equals', operand }`. Returns undefined
// for shapes we don't know how to translate to a single-member SQL predicate.
const extractEqualsValue = function (valueMatcher) {
	if (valueMatcher === null || valueMatcher === undefined) return undefined;
	if (typeof valueMatcher !== "object") return valueMatcher;
	if (valueMatcher.type === "equals" && valueMatcher.operand !== undefined) return valueMatcher.operand;
	return undefined;
};

// Recursively drop any filter node flagged `disabled: true`. Duplicate of the logic in
// adhoc-query-executor.js and adhoc-query-to-mdx.js on purpose — the SQL output should reflect
// what actually gets sent to the backend when the user runs the query, not the authored tree.
const stripDisabled = function (f) {
	if (!f || f.disabled) return null;
	if (f.type === "and" || f.type === "or") {
		const kept = (f.filters || []).map(stripDisabled).filter((c) => c !== null);
		if (kept.length === 0) return null;
		if (kept.length === 1) return kept[0];
		return { type: f.type, filters: kept };
	}
	if (f.type === "not") {
		const inner = stripDisabled(f.negated);
		if (!inner) return null;
		return { type: "not", negated: inner };
	}
	return f;
};

// Translate a (cleaned) filter tree to a SQL WHERE-clause fragment. Returns null if any node is
// not representable in the minimal dialect — the caller then emits a comment instead.
const filterToSql = function (f) {
	if (!f || !f.type) return null;
	if (f.type === "column") {
		const v = extractEqualsValue(f.valueMatcher);
		if (v === undefined) return null;
		if (v === null) return quoteId(f.column) + " IS NULL";
		return quoteId(f.column) + " = " + literal(v);
	}
	if (f.type === "and" || f.type === "or") {
		const parts = (f.filters || []).map(filterToSql);
		if (parts.some((p) => p === null)) return null;
		if (parts.length === 0) return null;
		if (parts.length === 1) return parts[0];
		const op = f.type === "and" ? " AND " : " OR ";
		// Parens around each child keep precedence correct regardless of the outer combiner.
		return parts.map((p) => "(" + p + ")").join(op);
	}
	if (f.type === "not") {
		const inner = filterToSql(f.negated);
		if (inner === null) return null;
		return "NOT (" + inner + ")";
	}
	return null;
};

// Format a single measure for the SELECT list. When `measureDefs` has a `.Aggregator` entry
// matching the measure name, render as `<AGG>("column") AS "measure"` — much closer to actual
// SQL than a bare identifier, since the Aggregator measure's semantics ARE a SQL aggregate.
// Otherwise fall back to the bare-identifier projection (the caller is expected to wrap it with
// the right SQL aggregate for their schema — same contract as before). Recognized aggregation
// keys (SUM, AVG, MIN, MAX, COUNT, COUNT_DISTINCT) are mapped to their SQL spelling; anything
// else is passed through as-is so non-standard aggregators still read sensibly.
const SQL_AGG_SPELLING = {
	SUM: "SUM",
	AVG: "AVG",
	MIN: "MIN",
	MAX: "MAX",
	COUNT: "COUNT",
	COUNT_DISTINCT: "COUNT(DISTINCT",
};
const formatMeasure = function (measureName, measureDefs) {
	const def = measureDefs && measureDefs[measureName];
	if (def && def.type === ".Aggregator" && def.columnName) {
		const agg = SQL_AGG_SPELLING[def.aggregationKey] || def.aggregationKey || "AGG";
		const col = quoteId(def.columnName);
		// `COUNT_DISTINCT` maps to `COUNT(DISTINCT col)` — the only spelling that needs a trailing
		// `)` rather than the default `<AGG>(<col>)`.
		const expr = agg === "COUNT(DISTINCT" ? agg + " " + col + ")" : agg + "(" + col + ")";
		return expr + " AS " + quoteId(measureName);
	}
	return quoteId(measureName);
};

// Main entry point. `queryModel` carries selectedMeasures / selectedColumnsOrdered /
// withStarColumns / filter; `cubeName` is embedded as the FROM target. `measureDefs` is an
// optional `{ [measureName]: measureDef }` map (typically `cube.measures` from the schema
// store) used to render `.Aggregator` measures as their SQL aggregate (SUM/AVG/…).
export const queryModelToSql = function (queryModel, cubeName, measureDefs) {
	const measures = Object.keys(queryModel.selectedMeasures || {}).filter((k) => queryModel.selectedMeasures[k] === true);
	const columns = (queryModel.selectedColumnsOrdered || []).slice();
	const withStar = queryModel.withStarColumns || {};

	// SELECT list: groupBy columns first (so the layout matches pivot-client expectations of
	// "dimensions on the left, measures on the right"), then measures.
	const selectItems = [];
	for (const c of columns) selectItems.push("  " + quoteId(c));
	for (const m of measures) selectItems.push("  " + formatMeasure(m, measureDefs));

	const lines = [];
	if (selectItems.length === 0) {
		// No columns, no measures — emit a degenerate but still-valid `SELECT *` so users get
		// something they can actually paste into a SQL client.
		lines.push("SELECT *");
	} else {
		lines.push("SELECT");
		lines.push(selectItems.join(",\n"));
	}
	lines.push("FROM " + quoteId(cubeName || ""));

	const effectiveFilter = stripDisabled(queryModel.filter);
	// A typeless filter (the default `{}` shape produced by queryHelper.makeQueryModel) means
	// "no predicate" — skip WHERE emission entirely, matching the MDX converter's behaviour.
	if (effectiveFilter && effectiveFilter.type) {
		const where = filterToSql(effectiveFilter);
		if (where !== null) {
			lines.push("WHERE " + where);
		} else {
			// Shape we can't yet translate — surface it as a SQL comment so a human can complete
			// the translation by hand, and so we don't silently produce a wrong predicate.
			lines.push("/* filter not representable in minimal SQL: " + JSON.stringify(effectiveFilter) + " */");
		}
	}

	if (columns.length > 0) {
		// withStar columns (grandTotal) get wrapped in ROLLUP() so a grand-total row is included
		// in the result set alongside the per-value rows. Most ANSI-SQL engines accept this;
		// dialect-specific alternatives are `GROUPING SETS((col),())` — left as future work.
		const groupParts = columns.map((c) => (withStar[c] ? "ROLLUP(" + quoteId(c) + ")" : quoteId(c)));
		lines.push("GROUP BY " + groupParts.join(", "));
	}

	return lines.join("\n");
};

export default { queryModelToSql };
