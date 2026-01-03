package eu.solven.adhoc.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Indicates a method or a class is a benchmark. It is complementary to unit-tests as they help detecting performance
 * regressions.
 * 
 * @author Benoit Lacelle
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@EnabledIf(TestAdhocIntegrationTests.ENABLED_IF)
@Tag("adhoc-benchmark")
public @interface AdhocBenchmark {

}
