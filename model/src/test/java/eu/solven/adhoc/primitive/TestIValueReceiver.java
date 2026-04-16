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
package eu.solven.adhoc.primitive;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIValueReceiver {
	@Test
	public void testOnPrimitives() {
		List<Object> list = new ArrayList<>();

		IValueReceiver receiver = new IValueReceiver() {

			@Override
			public void onObject(Object v) {
				list.add(v);
			}
		};

		receiver.onObject("foo");
		receiver.onLong(123);
		receiver.onDouble(12.34);

		Assertions.assertThat(list).containsExactly("foo", 123L, 12.34D);
	}

	@Test
	public void testInterceptOnObject_transformsObjectOnly() {
		List<Object> received = new ArrayList<>();

		IValueReceiver base = received::add;

		// Intercept: uppercase any String that arrives via onObject.
		IValueReceiver intercepted = base.interceptOnObject(v -> v instanceof String s ? s.toUpperCase() : v);

		intercepted.onObject("hello");
		intercepted.onLong(42L);
		intercepted.onDouble(3.14D);
		intercepted.onObject(999);

		// onObject values are transformed; onLong/onDouble pass through untouched (they skip the interceptor).
		Assertions.assertThat(received).containsExactly("HELLO", 42L, 3.14D, 999);
	}

	@Test
	public void testInterceptOnObject_longFallbackGoesThrough() {
		// When the base receiver does NOT override onLong, the default routes to onObject.
		// interceptOnObject must still intercept that routed call.
		List<Object> received = new ArrayList<>();

		// Base receiver that ONLY implements onObject — onLong/onDouble fall through to onObject.
		IValueReceiver base = received::add;

		IValueReceiver intercepted = base.interceptOnObject(v -> "wrapped:" + v);

		intercepted.onObject("direct");

		// onObject goes through the interceptor.
		Assertions.assertThat(received).containsExactly("wrapped:direct");
	}
}
