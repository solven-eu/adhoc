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
package eu.solven.adhoc.storage;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.sum.SumAggregator;

public class TestMultiTypeStorage {
	IAggregation sum = new SumAggregator();

	MultiTypeStorage<String> storage = MultiTypeStorage.<String>builder().aggregation(sum).build();

	@Test
	public void testIntAndLong() {
		storage.merge("k1", 123);
		storage.merge("k1", 234L);

		storage.onValue("k1", AsObjectValueConsumer.consumer(o -> {
			Assertions.assertThat(o).isEqualTo(357L);
		}));
	}

	@Test
	public void testIntAndDouble() {
		storage.merge("k1", 123);
		storage.merge("k1", 234.567D);

		storage.onValue("k1", AsObjectValueConsumer.consumer(o -> {
			Assertions.assertThat(o).isEqualTo(357.567D);
		}));
	}

	@Test
	public void testIntAndString() {
		storage.merge("k1", 123);
		storage.merge("k1", "234");
		storage.merge("k1", 345);

		storage.onValue("k1", AsObjectValueConsumer.consumer(o -> {
			Assertions.assertThat(o).isEqualTo(123 + "234" + 345);
		}));
	}

	@Test
	public void testStringAndString() {
		storage.merge("k1", "123");
		storage.merge("k1", "234");

		storage.onValue("k1", AsObjectValueConsumer.consumer(o -> {
			Assertions.assertThat(o).isEqualTo("123234");
		}));
	}

	@Test
	public void testClearKey() {
		storage.merge("k1", 123);
		storage.clearKey("k1");

		Assertions.assertThat(storage.size()).isEqualTo(0);
	}

	@Test
	public void testPutNull() {
		storage.merge("k1", 123);
		storage.put("k1", null);

		Assertions.assertThat(storage.size()).isEqualTo(0);
	}
}
