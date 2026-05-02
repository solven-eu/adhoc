/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.data.cell;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.primitive.IValueProvider;

/**
 * Tests for {@link ProxyValueReceiver}.
 *
 * @author Benoit Lacelle
 */
public class TestProxyValueReceiver {

	@Test
	public void testOnLong() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onLong(42L);

		Assertions.assertThat(IValueProvider.getValue(receiver.asValueProvider())).isEqualTo(42L);
	}

	@Test
	public void testOnDouble() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onDouble(3.14);

		Assertions.assertThat(IValueProvider.getValue(receiver.asValueProvider())).isEqualTo(3.14);
	}

	@Test
	public void testOnObject() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onObject("hello");

		Assertions.assertThat(IValueProvider.getValue(receiver.asValueProvider())).isEqualTo("hello");
	}

	@Test
	public void testOnLong_thenOnLong_coalesces() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onLong(1L);
		receiver.onLong(2L);

		// CoalesceAggregation keeps the first non-null value
		Assertions.assertThat(IValueProvider.getValue(receiver.asValueProvider())).isEqualTo(1L);
	}

	@Test
	public void testToString_beforeAnyValue() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		Assertions.assertThat(receiver.toString()).isEqualTo("recorded: null");
	}

	@Test
	public void testToString_afterLong() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onLong(99L);

		Assertions.assertThat(receiver.toString()).isEqualTo("recorded: 99");
	}

	@Test
	public void testToString_afterDouble() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onDouble(2.5);

		Assertions.assertThat(receiver.toString()).isEqualTo("recorded: 2.5");
	}

	@Test
	public void testToString_afterObject() {
		ProxyValueReceiver receiver = ProxyValueReceiver.builder().build();

		receiver.onObject("someValue");

		Assertions.assertThat(receiver.toString()).isEqualTo("recorded: someValue");
	}
}
