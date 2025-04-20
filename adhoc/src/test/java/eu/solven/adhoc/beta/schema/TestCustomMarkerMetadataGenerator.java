/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
