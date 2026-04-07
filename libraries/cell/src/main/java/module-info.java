/**
 * Cell layer: slice/coordinate model, map factories, column scanners, and engine query steps that bridge the encoding
 * layer to the query engine.
 */
module eu.solven.adhoc.cell {

	// ── Exported packages ────────────────────────────────────────────────────
	exports eu.solven.adhoc.cuboid;
	exports eu.solven.adhoc.cuboid.slice;
	exports eu.solven.adhoc.cuboid.tabular;
	exports eu.solven.adhoc.engine.step;
	exports eu.solven.adhoc.eventbus;
	exports eu.solven.adhoc.map;
	exports eu.solven.adhoc.map.factory;
	exports eu.solven.adhoc.map.keyset;
	exports eu.solven.adhoc.util.map;

	// ── Sibling modules ──────────────────────────────────────────────────────
	// adhoc-public: AdhocUnsafe, ILikeList, options.*, primitive.*
	requires eu.solven.adhoc.model;
	// adhoc-encoding: IAppendableTable, IHasIndexOf, encoding.*
	requires eu.solven.adhoc.encoding;
	// adhoc-filters: ISliceFilter, FilterBuilder, filter.value.*
	requires eu.solven.adhoc.query.filters;

	// ── Named third-party modules ────────────────────────────────────────────
	requires com.google.common;
	requires it.unimi.dsi.fastutil;
	requires eu.solven.pepper;
	requires eu.solven.pepper.mappath;

	// Runtime via Lombok @Slf4j
	requires org.slf4j;

	// https://stackoverflow.com/questions/47460373/module-info-java-does-not-work-with-lombok-in-java-9/59976234#59976234
	requires static lombok;

	// Annotation-only (compile-time)
	requires static com.fasterxml.jackson.annotation;
	requires static org.jspecify;
	requires static com.github.spotbugs.annotations;
	requires static com.google.errorprone.annotations;
}
