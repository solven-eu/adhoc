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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.Singular;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Standard {@link IConsumingStream}.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@SuperBuilder(toBuilder = true)
public class ConsumingStream<T> implements IConsumingStream<T> {
	@NonNull
	final Consumer<Consumer<T>> source;

	@Singular
	@NonNull
	final ImmutableList<Runnable> closeHandlers;

	// Not part of the builder: each instance gets its own fresh flag via field initialisation.
	// Ensures close() is idempotent when both auto-close-on-exception and try-with-resources fire.
	private final AtomicBoolean alreadyClosed = new AtomicBoolean(false);

	@Singular
	@NonNull
	final ImmutableList<Consumer<? super T>> peekers;

	@Override
	public void forEach(Consumer<T> consumer) {
		try {
			source.accept(tabularRecord -> {
				peekers.forEach(peeker -> peeker.accept(tabularRecord));

				consumer.accept(tabularRecord);
			});
		} finally {
			// Always close on exit: IConsumingStream is single-use and IO-bound sources must be released
			// immediately after the last element, whether forEach completed normally or threw.
			// alreadyClosed makes this idempotent, so a surrounding try-with-resources is still safe.
			close();
		}
	}

	@Override
	public void close() {
		if (!alreadyClosed.compareAndSet(false, true)) {
			return;
		}
		closeHandlers.reverse().forEach(closeHandler -> {
			try {
				closeHandler.run();
			} catch (Exception e) {
				log.warn("Error closing resource", e);
			}
		});
	}

	@Override
	public IConsumingStream<T> onClose(Runnable closeHandler) {
		return toBuilder().closeHandler(closeHandler).build();
	}
}
