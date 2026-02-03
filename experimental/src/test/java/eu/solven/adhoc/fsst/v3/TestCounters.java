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
package eu.solven.adhoc.fsst.v3;

import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounters {
	@Test
	public void testCount() {
		Counters c = new Counters();

		c.incSingle(3);
		c.incPair(5, 7);

		{
			AtomicInteger codeHolder = new AtomicInteger(0);
			Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(1);
			Assertions.assertThat(codeHolder).hasValue(3);
		}

		{
			AtomicInteger codeHolder = new AtomicInteger(17);
			Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(0);
			Assertions.assertThat(codeHolder).hasValue(IFsstConstants.fsstCodeMax);
		}
	}

	@Test
	public void testReset() {
		Counters c = new Counters();

		c.incSingle(3);
		c.incPair(5, 7);

		c.reset();

		AtomicInteger codeHolder = new AtomicInteger(0);
		Assertions.assertThat(c.nextNotZero(codeHolder)).isEqualTo(0);
		Assertions.assertThat(codeHolder).hasValue(IFsstConstants.fsstCodeMax);

		Assertions.assertThat(c.getPairList()).isEmpty();

	}
}
