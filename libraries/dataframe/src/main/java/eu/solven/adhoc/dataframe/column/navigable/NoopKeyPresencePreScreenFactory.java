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
package eu.solven.adhoc.dataframe.column.navigable;

/**
 * {@link IKeyPresencePreScreenFactory} that always returns {@link NoopKeyPresencePreScreen#instance()}, replicating the
 * legacy behavior of {@code MultitypeNavigableColumn} where {@code appendIfOptimal} always falls through to the exact
 * binary-search lookup. Useful as a benchmarking baseline and as a way to disable the pre-screen optimization without
 * touching call sites.
 *
 * @author Benoit Lacelle
 */
public final class NoopKeyPresencePreScreenFactory implements IKeyPresencePreScreenFactory {

	/** Shared singleton instance — stateless. */
	public static final NoopKeyPresencePreScreenFactory INSTANCE = new NoopKeyPresencePreScreenFactory();

	private NoopKeyPresencePreScreenFactory() {
		// Use NoopKeyPresencePreScreenFactory#INSTANCE.
	}

	@Override
	public <T> IKeyPresencePreScreen<T> create(int capacity) {
		return NoopKeyPresencePreScreen.instance();
	}
}
