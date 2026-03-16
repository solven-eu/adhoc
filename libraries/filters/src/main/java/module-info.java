/**
 * Column-oriented encoding layer: packed integers, dictionary encoding, FSST string compression, and appendable table
 * pages.
 */
module eu.solven.adhoc.query.filters {

	// ── Exported packages ────────────────────────────────────────────────────
	exports eu.solven.adhoc.query.filter;
	exports eu.solven.adhoc.query.filter.jackson;
	exports eu.solven.adhoc.query.filter.optimizer;
	exports eu.solven.adhoc.query.filter.stripper;
	exports eu.solven.adhoc.query.filter.value;

	// ── Sibling modules ──────────────────────────────────────────────────────
	// adhoc-public: AdhocUnsafe, ILikeList, options.*
	requires eu.solven.adhoc.meta;

	// ── Named third-party modules ────────────────────────────────────────────
	requires com.google.common;
	requires tools.jackson.databind;
	requires eu.solven.pepper;
	
	// Jackson needs reflective access to Lombok-generated builders for deserialization
	opens eu.solven.adhoc.query.filter to tools.jackson.databind;
	opens eu.solven.adhoc.query.filter.value to tools.jackson.databind;

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
