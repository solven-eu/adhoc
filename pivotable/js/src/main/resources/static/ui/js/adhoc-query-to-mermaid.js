// Minimal queryModel -> Mermaid flowchart converter. Sibling of `adhoc-query-to-sql.js` and
// `adhoc-query-to-mdx.js` — same informative-only contract: the rendered diagram is a visual
// projection of the JSON queryModel, not an execution plan.
//
// Layout: a left-to-right "sandwich" of three stages,
//
//    [ Filter ]  -->  [ Cube ]  -->  [ GroupBy ]  -->  [ Measures ]
//
// - Filter: nested subgraphs for AND/OR/NOT, leaf nodes for column predicates. Omitted entirely
//   when the filter is `{}` / missing / reduced to matchAll after disabled nodes are stripped.
// - Cube: a stadium-shaped node carrying the cube name (fallback: "cube" when none passed).
// - GroupBy: one node per selected column, stacked top-to-bottom. Columns with `withStar=true`
//   carry a trailing `*` to flag the grandTotal rollup.
// - Measures: one node per selected measure, stacked top-to-bottom.
//
// Design choices:
// - Dependency-free string generator — tested in vitest without a DOM; rendering via mermaid.js
//   is the caller's job (see `adhoc-query-raw-modal.js`).
// - Stable, deterministic node IDs (`f1`, `f2`, `g1`, `n1`, `gb_<col>`, `m_<measure>`) — makes
//   diffs predictable and tests simple. IDs are slugified to strip characters Mermaid rejects.
// - Subgraphs label themselves with their combiner (`Filter (AND)`, etc.) rather than carrying a
//   separate textual node, so the diagram stays compact.

// Chars Mermaid rejects (or that break node-id parsing). We replace them with `_`.
// See https://mermaid.js.org/syntax/flowchart.html#special-characters-that-break-syntax
const slugify = function (name) {
	return String(name).replace(/[^A-Za-z0-9_]/g, "_");
};

// Escape a value destined to appear inside a Mermaid node label (between double quotes). Mermaid
// labels don't support embedded `"`; the recommended workaround is HTML entity `&quot;`. We also
// escape `&` to `&amp;` so we don't accidentally form other entities. Newlines are rendered as
// `<br/>` which Mermaid supports inside quoted labels.
const escapeLabel = function (v) {
	return String(v).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/\n/g, "<br/>");
};

// Extract the primitive value carried by a ColumnFilter's `valueMatcher`. Mirrors the logic in
// the MDX/SQL converters — raw primitive or `{ type: 'equals', operand }`. Returns `undefined`
// for matchers we don't know how to render as a single-value predicate (they fall back to a
// `?` label so the diagram still shows the column being filtered).
const extractEqualsValue = function (valueMatcher) {
	if (valueMatcher === null || valueMatcher === undefined) return undefined;
	if (typeof valueMatcher !== "object") return valueMatcher;
	if (valueMatcher.type === "equals" && valueMatcher.operand !== undefined) return valueMatcher.operand;
	return undefined;
};

// Recursively drop any filter node flagged `disabled: true`. Matches `adhoc-query-executor.js`
// behaviour so the diagram reflects what would actually be sent to the backend.
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

// Format a column predicate (`col = value`) for display. `valueMatcher` shapes we can't unpack to
// a single value become a trailing `= ?` so the user still sees which column is being filtered.
const formatPredicate = function (f) {
	const v = extractEqualsValue(f.valueMatcher);
	if (v === undefined) return f.column + " = ?";
	if (v === null) return f.column + " IS NULL";
	return f.column + " = " + String(v);
};

// Recursive filter renderer. Pushes Mermaid lines into `out` and returns the ID of the node (or
// subgraph) representing this filter so the caller can wire an edge from it to the next stage.
// `counter` is a mutable { n: 0 } so every leaf/group gets a unique id regardless of tree shape.
const renderFilter = function (f, out, counter, indent) {
	const pad = "  ".repeat(indent);
	if (f.type === "column") {
		const id = "f" + ++counter.n;
		out.push(pad + id + '["' + escapeLabel(formatPredicate(f)) + '"]');
		return id;
	}
	if (f.type === "and" || f.type === "or") {
		const id = (f.type === "and" ? "gAnd" : "gOr") + ++counter.n;
		out.push(pad + "subgraph " + id + '["' + f.type.toUpperCase() + '"]');
		out.push(pad + "  direction TB");
		for (const child of f.filters || []) {
			renderFilter(child, out, counter, indent + 1);
		}
		out.push(pad + "end");
		return id;
	}
	if (f.type === "not") {
		const id = "gNot" + ++counter.n;
		out.push(pad + "subgraph " + id + '["NOT"]');
		out.push(pad + "  direction TB");
		if (f.negated) renderFilter(f.negated, out, counter, indent + 1);
		out.push(pad + "end");
		return id;
	}
	// Unknown shape — drop a placeholder leaf so the diagram at least shows something is there.
	const id = "fUnknown" + ++counter.n;
	out.push(pad + id + '["?"]');
	return id;
};

// Main entry point. Returns a Mermaid `flowchart LR` string. `cubeName` is optional — falls back
// to the literal "cube" so the diagram is still legible when the caller doesn't know the cube.
export const queryModelToMermaid = function (queryModel, cubeName) {
	const measures = Object.keys(queryModel.selectedMeasures || {}).filter((k) => queryModel.selectedMeasures[k] === true);
	const columns = (queryModel.selectedColumnsOrdered || []).slice();
	const withStar = queryModel.withStarColumns || {};
	const effectiveFilter = stripDisabled(queryModel.filter);

	const out = ["flowchart LR"];

	// ── Filter stage (optional) ──────────────────────────────────────────────────
	let filterExit = null;
	if (effectiveFilter && effectiveFilter.type) {
		const counter = { n: 0 };
		filterExit = renderFilter(effectiveFilter, out, counter, 1);
	}

	// ── Cube stage ───────────────────────────────────────────────────────────────
	const cubeId = "cube";
	const cubeLabel = cubeName ? String(cubeName) : "cube";
	out.push("  " + cubeId + '(["' + escapeLabel(cubeLabel) + '"])');

	// ── GroupBy stage ────────────────────────────────────────────────────────────
	const groupByIds = [];
	if (columns.length > 0) {
		out.push('  subgraph groupBy["Group By"]');
		out.push("    direction TB");
		for (const c of columns) {
			const id = "gb_" + slugify(c);
			const label = withStar[c] ? c + " *" : c;
			out.push("    " + id + '["' + escapeLabel(label) + '"]');
			groupByIds.push(id);
		}
		out.push("  end");
	}

	// ── Measures stage ───────────────────────────────────────────────────────────
	const measureIds = [];
	if (measures.length > 0) {
		out.push('  subgraph measures["Measures"]');
		out.push("    direction TB");
		for (const m of measures) {
			const id = "m_" + slugify(m);
			out.push("    " + id + '["' + escapeLabel(m) + '"]');
			measureIds.push(id);
		}
		out.push("  end");
	}

	// ── Edges: wire the stages together in sandwich order ───────────────────────
	if (filterExit) {
		out.push("  " + filterExit + " --> " + cubeId);
	}
	if (groupByIds.length > 0) {
		out.push("  " + cubeId + " --> groupBy");
	}
	if (measureIds.length > 0) {
		const prev = groupByIds.length > 0 ? "groupBy" : cubeId;
		out.push("  " + prev + " --> measures");
	}

	return out.join("\n");
};

export default { queryModelToMermaid };
