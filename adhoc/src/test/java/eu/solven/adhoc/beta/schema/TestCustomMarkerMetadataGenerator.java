package eu.solven.adhoc.beta.schema;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableSet;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TestCustomMarkerMetadataGenerator {
	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(CustomMarkerMetadata.class).verify();
	}

	@Test
	public void testCcy() {
		CustomMarkerMetadataGenerator generator = CustomMarkerMetadataGenerator.builder()
				.path("$.ccy")
				.possibleValues(() -> ImmutableSet.of("EUR", "USD"))
				.defaultValue(() -> Optional.of("EUR"))
				.build();

		CustomMarkerMetadata snapshot = generator.snapshot("someName");

		Assertions.assertThat(snapshot.getName()).isEqualTo("someName");
		Assertions.assertThat(snapshot.getPath()).isEqualTo("$.ccy");
		Assertions.assertThat(snapshot.getPossibleValues()).containsExactly("EUR", "USD");
		Assertions.assertThat(snapshot.getDefaultValue()).isEqualTo("EUR");
	}

	@Test
	public void testCcy_noDefault() {
		CustomMarkerMetadataGenerator generator = CustomMarkerMetadataGenerator.builder()
				.path("$.ccy")
				.possibleValues(() -> ImmutableSet.of("EUR", "USD"))
				.defaultValue(() -> Optional.empty())
				.build();

		CustomMarkerMetadata snapshot = generator.snapshot("someName");

		Assertions.assertThat(snapshot.getName()).isEqualTo("someName");
		Assertions.assertThat(snapshot.getPath()).isEqualTo("$.ccy");
		Assertions.assertThat(snapshot.getPossibleValues()).containsExactly("EUR", "USD");
		Assertions.assertThat(snapshot.getDefaultValue()).isNull();
	}
}
