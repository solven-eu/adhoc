/**
 * Structured query-plan representation. Used by the (future) EXPLAIN_V2 / EXPLAIN_ANALYZE_V2 options and by the Live
 * View. The intent is to hold the plan as a Java value tree (built by the existing
 * {@link eu.solven.adhoc.engine.observability.DagExplainer} and its perf-augmented subclass), so consumers (logs, UI
 * poll, UI streaming) can read structured data instead of parsing log strings.
 *
 * <p>
 * All types in this package are null-marked: parameters, return types and fields are non-null by default; explicit
 * {@link org.jspecify.annotations.Nullable @Nullable} marks the opt-outs.
 */
@NullMarked
package eu.solven.adhoc.engine.observability.plan;

import org.jspecify.annotations.NullMarked;
