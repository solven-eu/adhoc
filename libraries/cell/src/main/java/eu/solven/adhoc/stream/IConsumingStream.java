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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

/**
 * A lazy, sequential, closeable stream of elements of type {@code T}.
 *
 * <p>
 * Unlike {@link Stream}, this interface is designed for sources that expose their elements by accepting a
 * {@link Consumer Consumer&lt;T&gt;} — i.e. push-based sources — without requiring the elements to be materialised
 * upfront or split into sub-ranges. This makes it a natural fit for JDBC result sets, Arrow batch readers, and any
 * other IO-bound source that drives iteration internally.
 * </p>
 *
 * <p>
 * <b>Why not {@link Stream}?</b> {@link Stream#parallel()} routes work through a {@link ForkJoinPool}, which is
 * optimised for CPU-bound work-stealing. IO-bound sources (JDBC, Arrow, HTTP) must never run under FJP: blocking a
 * carrier thread shrinks pool parallelism and can deadlock. Additionally, {@link java.util.Spliterator#trySplit()} is
 * called eagerly by {@link Stream}, before any data is available, which degrades to sequential for sources that cannot
 * partition upfront (e.g. an {@code ArrowReader} whose batches arrive one at a time). This interface avoids both
 * problems by keeping iteration sequential and delegating concurrency decisions to the call site.
 * </p>
 *
 * <p>
 * <b>Composability.</b> Intermediate operations ({@link #filter}, {@link #map}, {@link #peek}) return a new
 * {@code IAdhocStream} that wraps the previous one; no work is done until {@link #forEach} is called. {@link #map}
 * supports cross-type transformations ({@code IAdhocStream&lt;T&gt; → IAdhocStream&lt;R&gt;}).
 * </p>
 *
 * <p>
 * <b>Lifecycle.</b> Call {@link #close()} after consumption to release underlying resources (connections, file
 * handles). Use {@link #onClose(Runnable)} to register cleanup hooks that fire in reverse registration order, matching
 * {@link Stream} semantics.
 * </p>
 *
 * @param <T>
 *            the element type
 * @author Benoit Lacelle
 */
public interface IConsumingStream<T> extends Consumer<Consumer<T>>, AutoCloseable {

	/**
	 * Passes each element to {@code consumer}, in encounter order.
	 *
	 * <p>
	 * This is the terminal operation that executes the full pipeline. It is equivalent to {@link Stream#forEach} but
	 * always sequential and safe to call from a virtual thread.
	 * </p>
	 *
	 * @param consumer
	 *            the action to perform on each element
	 */
	void forEach(Consumer<T> consumer);

	@Override
	default void accept(Consumer<T> t) {
		forEach(t);
	}

	@Override
	void close();

	default IConsumingStream<T> peek(Consumer<? super T> peeker) {
		return ConsumingStream.<T>builder().source(this).peeker(peeker).build();
	}

	default IConsumingStream<T> onClose(Runnable closeHandler) {
		return ConsumingStream.<T>builder().source(this).closeHandler(closeHandler).build();
	}

	/**
	 * Returns a new stream containing only the elements that match {@code predicate}.
	 *
	 * <p>
	 * This is an intermediate operation: the predicate is evaluated lazily, once per element, during {@link #forEach}.
	 * Elements for which the predicate returns {@code false} are dropped and never passed to downstream operations.
	 * </p>
	 *
	 * @param predicate
	 *            a non-interfering, stateless predicate to apply to each element
	 * @return a new {@code IAdhocStream} that emits only the matching elements
	 */
	default IConsumingStream<T> filter(Predicate<? super T> predicate) {
		return FilteringConsumingStream.<T>builder().source(this).predicate(predicate).build();
	}

	/**
	 * Returns a new stream whose elements are the result of applying {@code function} to each element of this stream.
	 *
	 * <p>
	 * This is an intermediate operation: the function is applied lazily, once per element, during {@link #forEach}.
	 * Unlike {@link Stream#map}, the output type {@code R} may differ from the input type {@code T}, enabling
	 * cross-type transformations (e.g. {@code IAdhocStream<Integer>} to {@code IAdhocStream<String>}).
	 * </p>
	 *
	 * @param <R>
	 *            the element type of the resulting stream
	 * @param function
	 *            a non-interfering, stateless function to apply to each element
	 * @return a new {@code IAdhocStream<R>} whose elements are the mapped values
	 */
	default <R> IConsumingStream<R> map(Function<? super T, R> function) {
		return MappingConsumingStream.<T, R>builder().upstream(this).function(function).build();
	}

	default IConsumingStream<T> distinct() {
		// Use a stateful predicate: ConcurrentHashMap.newKeySet().add returns true only for first-seen elements.
		// IConsumingStream is single-use, so sharing the seen-set across forEach calls is not a concern.
		return filter(ConcurrentHashMap.newKeySet()::add);
	}

	default long count() {
		AtomicLong counter = new AtomicLong();

		this.forEach(_ -> counter.incrementAndGet());

		return counter.get();
	}

	default Optional<T> findAny() {
		AtomicReference<T> ref = new AtomicReference<>();

		// BEWARE We will iterate along all entries, no short-circuit
		forEach(ref::set);

		return Optional.ofNullable(ref.get());
	}

	static <T> IConsumingStream<T> fromStream(Stream<T> stream) {
		return ConsumingStream.<T>builder().source(stream::forEach).build();
	}

	static <T> IConsumingStream<T> fromStream(List<IConsumingStream<T>> list) {
		// TODO Enable concurrency
		return ConsumingStream.<T>builder().source(s -> list.forEach(c -> c.forEach(s))).build();
	}

	static <T> IConsumingStream<T> empty() {
		return ConsumingStream.<T>builder().source(ImmutableList.<T>of()::forEach).build();
	}

	default List<T> toList() {
		List<T> list = Collections.synchronizedList(new ArrayList<>());

		forEach(list::add);

		return list;
	}
}
