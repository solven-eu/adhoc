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
package eu.solven.adhoc.stream;

import java.util.function.Consumer;
import java.util.function.Function;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Use a {@link Function} to map elements of one type to another, supporting cross-type transformations.
 *
 * @param <S>
 *            the element type of the upstream stream
 * @param <T>
 *            the element type produced by this stream
 * @author Benoit Lacelle
 */
@Slf4j
@Builder(toBuilder = true)
public class MappingConsumingStream<S, T> implements IConsumingStream<T> {

	@NonNull
	IConsumingStream<S> upstream;

	@NonNull
	Function<? super S, T> function;

	@Override
	public void forEach(Consumer<T> consumer) {
		try {
			upstream.forEach(in -> {
				T mapped = function.apply(in);

				consumer.accept(mapped);
			});
		} finally {
			// Always close on exit, mirroring ConsumingStream semantics.
			// upstream.close() is idempotent so a surrounding try-with-resources is still safe.
			close();
		}
	}

	@Override
	public void close() {
		upstream.close();
	}

}
