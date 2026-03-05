module eu.solven.adhoc.fsst {
	exports eu.solven.adhoc.encoding.bytes;
	exports eu.solven.adhoc.encoding.fsst;

	// Runtime dependency (ByteArrayList etc.)
	requires it.unimi.dsi.fastutil;
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
