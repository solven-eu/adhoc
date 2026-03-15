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
package eu.solven.adhoc.encoding.column;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAdhocColumnUnsafe {
	@BeforeEach
	public void resetProperties() {
		System.clearProperty("adhoc.limitColumnSize");
		System.clearProperty("adhoc.defaultColumnCapacity");

		AdhocColumnUnsafe.reloadProperties();
	}

	@Test
	public void testDefaults() {
		Assertions.assertThat(AdhocColumnUnsafe.getLimitColumnSize()).isEqualTo(1_000_000);
		Assertions.assertThat(AdhocColumnUnsafe.getDefaultColumnCapacity()).isEqualTo(1_000_000);
	}

	@Test
	public void testLimitColumnSize() {
		String someKey = "adhoc.limitColumnSize";

		System.setProperty(someKey, "123");
		AdhocColumnUnsafe.reloadProperties();

		Assertions.assertThat(AdhocColumnUnsafe.getLimitColumnSize()).isEqualTo(123);
		Assertions.assertThat(AdhocColumnUnsafe.getDefaultColumnCapacity()).isEqualTo(123);
	}

	@Test
	public void testDefaultCapacity_property() {
		String someKey = "adhoc.defaultColumnCapacity";

		System.setProperty(someKey, "123");
		AdhocColumnUnsafe.reloadProperties();

		Assertions.assertThat(AdhocColumnUnsafe.getLimitColumnSize()).isEqualTo(1_000_000);
		Assertions.assertThat(AdhocColumnUnsafe.getDefaultColumnCapacity()).isEqualTo(123);
	}

	@Test
	public void testDefaultCapacity_programmatic() {
		AdhocColumnUnsafe.setDefaultColumnCapacity(123);

		Assertions.assertThat(AdhocColumnUnsafe.getLimitColumnSize()).isEqualTo(1_000_000);
		Assertions.assertThat(AdhocColumnUnsafe.getDefaultColumnCapacity()).isEqualTo(123);
	}

}
