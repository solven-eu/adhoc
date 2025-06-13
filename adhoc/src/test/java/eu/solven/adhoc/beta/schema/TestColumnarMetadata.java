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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestColumnarMetadata {
	@BeforeEach
	public void clearWarns() {
		ColumnarMetadata.clearWarns();
	}

	@Test
	public void testKnownTypes() {
		Assertions.assertThat(toApiTypes(Map.of("someName", String.class))).isEqualTo(Map.of("someName", "varchar"));

		Assertions.assertThat(toApiTypes(Map.of("someName", Integer.class))).isEqualTo(Map.of("someName", "integer"));
		Assertions.assertThat(toApiTypes(Map.of("someName", Long.class) )).isEqualTo(Map.of("someName", "bigint"));

		Assertions.assertThat(toApiTypes(Map.of("someName", Float.class))).isEqualTo(Map.of("someName", "float"));
		Assertions.assertThat(toApiTypes(Map.of("someName", Double.class))).isEqualTo(Map.of("someName", "double"));
		Assertions.assertThat(toApiTypes(Map.of("someName", BigDecimal.class))).isEqualTo(Map.of("someName", "double"));

		Assertions.assertThat(toApiTypes(Map.of("someName", java.sql.Date.class)))
				.isEqualTo(Map.of("someName", "date"));
		Assertions.assertThat(toApiTypes(Map.of("someName", java.util.Date.class)))
				.isEqualTo(Map.of("someName", "date"));
		Assertions.assertThat(toApiTypes(Map.of("someName", LocalDate.class))).isEqualTo(Map.of("someName", "date"));

		Assertions.assertThat(toApiTypes(Map.of("someName", OffsetDateTime.class)))
				.isEqualTo(Map.of("someName", "timestamp with time zone"));

		Assertions.assertThat(ColumnarMetadata.UNCLEAR_TYPE_WARNED).isEmpty();
	}

	private Map<String, String> toApiTypes(Map<String, Class<?>> toString2) {
		return ColumnarMetadata.from(toString2).build().getColumnToTypes();
	}

	private static class SomeCustomClass {

	}

	@Test
	public void testUnknownTypes() {
		Assertions.assertThat(toApiTypes(Map.of("someName", SomeCustomClass.class)))
				.isEqualTo(Map.of("someName", "blob"));

		Assertions.assertThat(ColumnarMetadata.UNCLEAR_TYPE_WARNED)
				.contains(Map.entry("someName", SomeCustomClass.class));
	}
}
