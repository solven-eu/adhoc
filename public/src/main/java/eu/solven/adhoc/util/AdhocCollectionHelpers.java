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
package eu.solven.adhoc.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import lombok.experimental.UtilityClass;

/**
 * Some utilities around {@link Collection}.
 * 
 * @author Benoit Lacelle
 * @see BenchmarkCollectionUnnesting
 */
@UtilityClass
public class AdhocCollectionHelpers {
	/**
	 * 
	 * @param collection
	 * @return a {@link List} with no {@link Collection}, as input {@link Collection} element has been unnested.
	 */
	public static List<Object> unnestAsList(Collection<?> collection) {
		return unnestAsList(collection, Predicates.alwaysTrue());
	}

	public static List<Object> unnestAsList(Collection<?> collection, Predicate<Object> acceptElement) {
		// Capacity 1 as we optimistically assume the input collection has no Collection
		// The actual needed capacity is at most the depth of nesting
		Deque<Iterator<?>> nested = new ArrayDeque<>(1);
		nested.add(collection.iterator());

		// In many case, there is nothing to unnest
		// Make sure this implementation accepts null
		List<Object> output = new ArrayList<>(collection.size());

		outerLoop: while (!nested.isEmpty()) {
			Iterator<?> iterator = nested.pollFirst();

			while (iterator.hasNext()) {
				Object element = iterator.next();

				if (element instanceof Collection<?> c) {
					// Add the nested iterator in priority
					nested.addLast(c.iterator());

					// Add back current iterator as next in loop
					if (iterator.hasNext()) {
						nested.addLast(iterator);
					}
					// Reset the loop given the updated queue of iterators
					continue outerLoop;
				} else if (acceptElement.test(element)) {
					output.add(element);
				}
			}
		}

		return output;
	}

	public static <E> List<E> unnestAsList2(Collection<E> collection, Predicate<Object> acceptElement) {
		return unnestAsList(Collection.class, collection, Collection::size, c -> c, acceptElement);
	}

	public static <C, E> List<E> unnestAsList(Class<C> clazz,
			C collection,
			ToIntFunction<C> sizeFunction,
			Function<C, Collection<? extends E>> flatMapper,
			Predicate<Object> acceptElement) {
		// Capacity 1 as we optimistically assume the input collection has no Collection
		// The actual needed capacity is at most the depth of nesting
		Deque<Iterator<E>> nested = new ArrayDeque<>(1);
		nested.add((Iterator<E>) flatMapper.apply(collection).iterator());

		// In many case, there is nothing to unnest
		ImmutableList.Builder<E> output = ImmutableList.builderWithExpectedSize(sizeFunction.applyAsInt(collection));

		outerLoop: while (!nested.isEmpty()) {
			Iterator<E> iterator = nested.pollFirst();

			while (iterator.hasNext()) {
				E element = iterator.next();

				if (element instanceof Collection<?> c) {
					// Add the nested iterator in priority
					nested.addLast((Iterator<E>) c.iterator());

					// Add back current iterator as next in loop
					if (iterator.hasNext()) {
						nested.addLast(iterator);
					}
					// Reset the loop given the updated queue of iterators
					continue outerLoop;
				} else if (acceptElement.test(element)) {
					output.add(element);
				}
			}
		}

		return output.build();
	}
}
