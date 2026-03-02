package eu.solven.adhoc;

import java.io.IOException;
import java.net.URISyntaxException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public class TestJUnitConfiguration {
	// Ensures `junit-platform.properties` is present only once, typically only from adhoc-public
	@Test
	public void testJUnitPlateformProperties() throws IOException, URISyntaxException {
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

		Resource[] resources = resolver.getResources("classpath*:junit-platform.properties");

		Assertions.assertThat(resources).hasSize(1);
	}
}
