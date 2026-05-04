/**
 * Column-oriented encoding layer: packed integers, dictionary encoding, FSST string compression, and appendable table
 * pages.
 */
module eu.solven.adhoc.model {

	// ── Exported packages ────────────────────────────────────────────────────
	exports eu.solven.adhoc.collection;
	exports eu.solven.adhoc.model.column;
	exports eu.solven.adhoc.model.measure;
	exports eu.solven.adhoc.options;
	exports eu.solven.adhoc.primitive;
	exports eu.solven.adhoc.model.query;
	exports eu.solven.adhoc.model.query.groupby;
	exports eu.solven.adhoc.resource;
	exports eu.solven.adhoc.util;
	exports eu.solven.adhoc.util.cache;
	exports eu.solven.adhoc.util.immutable;

	// Jackson needs reflective access to Lombok-generated builders for deserialization
	opens eu.solven.adhoc.model.measure to tools.jackson.databind;
	opens eu.solven.adhoc.model.query to tools.jackson.databind;
	opens eu.solven.adhoc.model.query.groupby to tools.jackson.databind;

	// ── Named third-party modules ────────────────────────────────────────────
	requires com.google.common;
	requires tools.jackson.databind;
	requires eu.solven.pepper;

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
