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
package eu.solven.adhoc.engine.tabular.optimizer;

import java.util.concurrent.Callable;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A helper test class for test which can be turned into long-running profilable benchmarks.
 */
public abstract class ABenchmarkable {

	public abstract int nbIterations();

	/**
	 * Let's ensure the loop size is forced to 1 in CI, to prevent CI to be cluttered by long-running benchmarks.
	 */
	@Test
	public void testCIisFine() {
		Assertions.assertThat(nbIterations()).isEqualTo(1);
	}

	public void doBenchmark(Runnable r) {
		int nbIterations = nbIterations();
		for (int i = 0; i < nbIterations; i++) {
			r.run();
		}
	}

	public void doBenchmark(Callable<?> c) {
		int nbIterations = nbIterations();
		for (int i = 0; i < nbIterations; i++) {
			try {
				c.call();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
