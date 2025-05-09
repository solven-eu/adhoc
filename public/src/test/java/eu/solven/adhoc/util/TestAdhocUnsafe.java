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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAdhocUnsafe {
	@BeforeEach
	public void resetProperties() {
		System.clearProperty("adhoc.limitColumnSize");

		AdhocUnsafe.reloadProperties();
	}

	@Test
	public void testDefaults() {
		Assertions.assertThat(AdhocUnsafe.limitColumnSize).isEqualTo(1_000_000);
		Assertions.assertThat(AdhocUnsafe.limitOrdinalToString).isEqualTo(5);
		Assertions.assertThat(AdhocUnsafe.limitCoordinates).isEqualTo(100);
		Assertions.assertThat(AdhocUnsafe.failFast).isEqualTo(true);
		Assertions.assertThat(AdhocUnsafe.defaultCapacity()).isEqualTo(1_000_000);
	}

	@Test
	public void testParse() {
		String someKey = "TestAdhocUnsafe.testProperty";

		// The `_` syntax is not managed by `Integer.getInteger`
		System.setProperty(someKey, "10_000_000");
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(123);

		System.setProperty(someKey, "10000000");
		Assertions.assertThat(AdhocUnsafe.safeLoadIntegerProperty(someKey, 123)).isEqualTo(10_000_000);
	}

	@Test
	public void testLimitColumnSize() {
		String someKey = "adhoc.limitColumnSize";

		System.setProperty(someKey, "123");
		AdhocUnsafe.reloadProperties();

		Assertions.assertThat(AdhocUnsafe.limitColumnSize).isEqualTo(123);
		Assertions.assertThat(AdhocUnsafe.defaultCapacity()).isEqualTo(123);
	}
}
