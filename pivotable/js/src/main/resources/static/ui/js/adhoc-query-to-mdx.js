// Minimal queryModel -> MDX converter. Intended for side-by-side comparison with MDX-native
// OLAP clients (Excel, Mondrian, Atoti, etc.), not for back-and-forth round-tripping of every
// possible filter shape. The code lives on the JS/Pivotable side intentionally — we don't want
// to introduce a backend API just for this edge-case; keeping it here means the Pivotable UI
// can expose it behind a button and evolve it without a deploy.
//
// MDX primer (for future edits):
//   SELECT
//     {<members on COLUMNS>} ON COLUMNS,
//     {<members on ROWS>}    ON ROWS
//   FROM [<cube>]
//   WHERE (<slicer tuple>)
//
// Conventions used here:
//   - Each column is assumed to map to a single-level hierarchy named after the column, so a
//     member is `[col].[col].[value]` and the members list is `[col].[col].Members`.
//   - Measures are always under the `[Measures]` dimension: `[Measures].[name]`.
//   - Cross-joins on ROWS use the CrossJoin function form (not the `*` shorthand), with each
//     operand on its own indented line so multi-column groupBys stay readable.
//   - The ROWS axis is prefixed with `NON EMPTY` so tuples with all-null measure values are
//     dropped — matches what OLAP-style pivot clients show by default.
//   - Identifiers inside square brackets escape `]` by doubling it (MDX convention).
//   - Slicer members in WHERE use the Atoti path shape `[Dim].[AllMember].[value]`, because
//     Atoti serializes single-level hierarchies with the AllMember sitting between the
//     dimension and the leaf. Change this in one place if a different dialect is needed.
//
// Out of scope for the minimal version (falls back to a `/* … */` comment in the output):
//   - OR across distinct columns (requires an MDX set on a row axis).
//   - NOT / LikeMatcher / InMatcher — could be added with EXCEPT / set operators later.
//   - customMarkers (no native MDX equivalent).
//
// Handled:
//   - withStarColumns (grandTotal) — rendered as
//       DrillDownMember({[col].[col].[AllMember]}, {[col].[col].[AllMember]})
//     so the AllMember is emitted first, followed by the children drill-down.
//     Uses the Atoti naming convention `[AllMember]` rather than the Microsoft-default
//     `[All]` — adjust here (and update tests) if a different dialect is needed.

// Escape `]` in an identifier by doubling it; MDX delimits names with square brackets.
const esc = function (name) {
	return String(name).replace(/]/g, "]]");
};

const asMeasureMember = function (measureName) {
	return "[Measures].[" + esc(measureName) + "]";
};

const asLevelMembers = function (columnName) {
	return "{[" + esc(columnName) + "].[" + esc(columnName) + "].Members}";
};

// withStarColumns variant: emit the column's AllMember plus a drill-down to its children.
// Semantics match what pivot clients render for a "grand total + per-value" row: the AllMember
// tuple aggregates everything, followed by the individual level members.
// `AllMember` (not `All`) follows the Atoti naming convention for the root member.
const asLevelMembersWithStar = function (columnName) {
	const all = "[" + esc(columnName) + "].[" + esc(columnName) + "].[AllMember]";
	return "DrillDownMember({" + all + "}, {" + all + "})";
};

// Pick the right set expression for a single row column based on whether the user ticked
// the grandTotal (withStar) switch for it.
const rowSetFor = function (columnName, withStar) {
	return withStar ? asLevelMembersWithStar(columnName) : asLevelMembers(columnName);
};

// Slicer member path for WHERE. Atoti serializes `[Dim].[AllMember].[value]` for a leaf of
// a single-level hierarchy — the AllMember sits between the dim name and the leaf value.
const asMember = function (columnName, value) {
	return "[" + esc(columnName) + "].[AllMember].[" + esc(value) + "]";
};

// Extract the primitive value carried by a ColumnFilter's `valueMatcher`. The pivotable
// model uses a small variety of shapes depending on the UI that produced the filter:
//   - A raw primitive       -> "France"
//   - An "equals" object    -> { type: "equals", operand: "France" }
// Returns undefined for shapes we don't know how to render as a single MDX member (in/like/not/etc.).
const extractEqualsValue = function (valueMatcher) {
	if (valueMatcher === null || valueMatcher === undefined) return undefined;
	if (typeof valueMatcher !== "object") return valueMatcher;
	if (valueMatcher.type === "equals" && valueMatcher.operand !== undefined) return valueMatcher.operand;
	return undefined;
};

// Recursively drop any filter node flagged `disabled: true`. Duplicate of the logic in
// adhoc-query-executor.js on purpose — the MDX output should reflect what actually gets
// sent to the backend when the user runs the query, not the full authored tree.
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

// Collect [{column, value}] pairs if the filter is an AND of column-equality clauses, a
// single column-equality, or empty. Returns null if anything unrepresentable is encountered
// (OR, NOT, non-equality matcher, …) — the caller then emits a comment instead of a WHERE.
const extractAndEquals = function (filter) {
	if (!filter || !filter.type) return [];
	if (filter.type === "column") {
		const v = extractEqualsValue(filter.valueMatcher);
		if (v === undefined) return null;
		return [{ column: filter.column, value: v }];
	}
	if (filter.type === "and") {
		let all = [];
		for (const child of filter.filters || []) {
			const part = extractAndEquals(child);
			if (part === null) return null;
			all = all.concat(part);
		}
		return all;
	}
	return null;
};

// Main entry point. `queryModel` carries selectedMeasures / selectedColumnsOrdered / filter;
// `cubeName` is embedded as the FROM target.
export const queryModelToMdx = function (queryModel, cubeName) {
	const measures = Object.keys(queryModel.selectedMeasures || {}).filter((k) => queryModel.selectedMeasures[k] === true);
	const columns = (queryModel.selectedColumnsOrdered || []).slice();

	const lines = ["SELECT"];
	const axes = [];

	if (measures.length > 0) {
		axes.push("  {" + measures.map(asMeasureMember).join(", ") + "} ON COLUMNS");
	}
	const withStarColumns = queryModel.withStarColumns || {};
	if (columns.length === 1) {
		// Single column: keep the expression inline after NON EMPTY.
		axes.push("  NON EMPTY " + rowSetFor(columns[0], !!withStarColumns[columns[0]]) + " ON ROWS");
	} else if (columns.length > 1) {
		// Multi-column CrossJoin: one operand per line so long groupBys stay readable.
		const parts = columns.map((c) => rowSetFor(c, !!withStarColumns[c]));
		axes.push("  NON EMPTY CrossJoin(\n    " + parts.join(",\n    ") + "\n  ) ON ROWS");
	}

	if (axes.length > 0) {
		lines.push(axes.join(",\n"));
	}

	lines.push("FROM [" + esc(cubeName || "") + "]");

	const effectiveFilter = stripDisabled(queryModel.filter);
	if (effectiveFilter) {
		const equals = extractAndEquals(effectiveFilter);
		if (equals && equals.length === 1) {
			// Single-member WHERE stays inline — a single line is easier to read than a
			// three-line block carrying one member.
			lines.push("WHERE (" + asMember(equals[0].column, equals[0].value) + ")");
		} else if (equals && equals.length > 1) {
			// Multiple members: each on its own indented line, mirroring the multi-line
			// CrossJoin layout so long filters stay readable.
			const members = equals.map((e) => "  " + asMember(e.column, e.value));
			lines.push("WHERE (\n" + members.join(",\n") + "\n)");
		} else if (equals === null) {
			// Shape we can't yet translate — surface it as an MDX comment so a human can
			// complete the translation by hand, and so we don't silently produce a wrong query.
			lines.push("/* filter not representable in minimal MDX: " + JSON.stringify(effectiveFilter) + " */");
		}
		// equals === [] is the "empty filter" case — no WHERE clause needed.
	}

	return lines.join("\n");
};

export default { queryModelToMdx };
