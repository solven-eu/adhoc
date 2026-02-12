package eu.solven.adhoc.pivotable.app;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.pepper.spring.PepperResourceHelper;

public class TestPivotableResources {
	@Test
	public void testApplicationNoProfile() {
		// As pivotable is often used as library, it should not provide a default application.yml, which would conflict
		// when the project one
		Assertions.assertThatThrownBy(() -> PepperResourceHelper.loadAsString("application.yml"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Can not find: application.yml");

		Assertions.assertThat(PepperResourceHelper.loadAsString("application-pivotable.yml")).contains("spring:");

		Assertions.assertThat(IPivotableSpringProfiles.C_CONFIG)
				.startsWith("classpath:")
				.endsWith("pivotable-config.yml");
		Assertions.assertThat(PepperResourceHelper.loadAsString("pivotable-config.yml")).contains("spring:");
	}
}
