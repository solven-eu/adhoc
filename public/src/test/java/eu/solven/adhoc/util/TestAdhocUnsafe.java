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
package eu.solven.adhoc.util;

import java.util.Comparator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAdhocUnsafe {
	@BeforeEach
	public void resetProperties() {
		System.clearProperty("adhoc.limitColumnSize");
		System.clearProperty("adhoc.defaultColumnCapacity");

		AdhocUnsafe.reloadProperties();
	}

	@Test
	public void testDefaults() {
		Assertions.assertThat(AdhocUnsafe.getLimitOrdinalToString()).isEqualTo(16);
		Assertions.assertThat(AdhocUnsafe.isFailFast()).isEqualTo(true);
	}

	@Test
	public void testParse_integer() {
		String someKey = "TestAdhocUnsafe.testProperty";

		// The `_` syntax is not managed by `Integer.getInteger`
		System.setProperty(someKey, "10_000_000");
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(123);

		System.setProperty(someKey, "10000000");
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(10_000_000);

		System.setProperty(someKey, "notAnInteger");
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(123);

		System.clearProperty(someKey);
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(123);
	}

	@Test
	public void testParse_boolean() {
		String someKey = "TestAdhocUnsafe.testProperty";

		System.setProperty(someKey, "true");
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, true)).isEqualTo(true);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, false)).isEqualTo(true);

		System.setProperty(someKey, "True");
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, true)).isEqualTo(true);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, false)).isEqualTo(true);

		System.setProperty(someKey, "false");
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, true)).isEqualTo(false);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, false)).isEqualTo(false);

		// BEWARE Boolean.parse accepts only "true"
		System.setProperty(someKey, "notBoolean");
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, true)).isEqualTo(false);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, false)).isEqualTo(false);

		System.clearProperty(someKey);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, true)).isEqualTo(true);
		Assertions.assertThat(AdhocUnsafe.safeLoadBooleanProperty(someKey, false)).isEqualTo(false);
	}

	@Test
	public void testToggleFailfast() {
		Assertions.assertThat(AdhocUnsafe.isFailFast()).isTrue();
		AdhocUnsafe.outFailFast();
		Assertions.assertThat(AdhocUnsafe.isFailFast()).isFalse();
		AdhocUnsafe.inFailFast();
		Assertions.assertThat(AdhocUnsafe.isFailFast()).isTrue();
	}

	@Test
	public void testNullComparator() {
		// By default, null is last
		Assertions.assertThat(AdhocUnsafe.getNullComparator().compare("a", null)).isNegative();
		Assertions.assertThat(AdhocUnsafe.getValueComparator().compare("a", null)).isNegative();

		try {
			AdhocUnsafe.setNullComparator(Comparator.nullsFirst((Comparator) Comparator.naturalOrder()));

			Assertions.assertThat(AdhocUnsafe.getNullComparator().compare("a", null)).isPositive();
			Assertions.assertThat(AdhocUnsafe.getValueComparator().compare("a", null)).isPositive();
		} finally {
			AdhocUnsafe.resetProperties();
		}

		// Ensure resetProperties did reset nullComparator
		Assertions.assertThat(AdhocUnsafe.getNullComparator().compare("a", null)).isNegative();
		Assertions.assertThat(AdhocUnsafe.getValueComparator().compare("a", null)).isNegative();
	}

	@Test
	public void testResetUUIDs() {
		Assertions.assertThat(AdhocUnsafe.randomUUID().toString()).doesNotStartWith("00000000-0000-");

		AdhocUnsafe.resetDeterministicQueryIds();

		Assertions.assertThat(AdhocUnsafe.nextQueryIndex()).isEqualTo(0);
		Assertions.assertThat(AdhocUnsafe.nextQueryStepIndex()).isEqualTo(0);
		Assertions.assertThat(AdhocUnsafe.randomUUID()).hasToString("00000000-0000-0000-0000-000000000000");
		Assertions.assertThat(AdhocUnsafe.randomUUID()).hasToString("00000000-0000-0000-0000-000000000001");
	}
}
