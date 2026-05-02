module eu.solven.adhoc.pivotable.infra {
	exports eu.solven.adhoc.app;
	exports eu.solven.adhoc.security;
	exports eu.solven.adhoc.tools;

	// Runtime via Lombok @Slf4j
	requires org.slf4j;

	requires spring.beans;
	requires spring.context;
	requires spring.core;
	requires com.fasterxml.uuid;

	// Annotation-only dependencies — compile time only
	requires static lombok;
	requires static com.github.spotbugs.annotations;
}
