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

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;

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
	public static Collection<?> unnestAsCollection(Collection<?> collection) {
		return unnestAsCollection(collection, Predicates.alwaysTrue());
	}

	public static Collection<?> unnestAsCollection(Collection<?> collection, Predicate<Object> acceptElement) {
		if (collection.isEmpty()) {
			return collection;
		}

		// Optimistic path: manage the case we receive a single entry
		{
			if (collection.size() == 1) {
				Object singleElement = collection.iterator().next();
				if (singleElement instanceof Collection<?> singleCollection) {
					return unnestAsCollection(singleCollection);
				}
			}
		}
		// Optimistic path: in most cases, there is no nesting
		{
			boolean hasNested = false;
			for (Object item : collection) {
				if (item instanceof Collection<?>) {
					hasNested = true;
					break;
				}
			}

			if (!hasNested) {
				return collection;
			}
		}

		return unnestAsList(collection, acceptElement);
	}

	public static <E> List<E> unnestAsList(Collection<? extends E> collection, Predicate<? super E> acceptElement) {
		return unnestAsList(Collection.class, collection, Collection::size, c -> c, acceptElement);
	}

	public static <C, E> List<E> unnestAsList(Class<C> clazz,
			C collection,
			ToIntFunction<C> sizeFunction,
			Function<C, Collection<? extends E>> flatMapper,
			Predicate<? super E> acceptElement) {
		// Capacity 1 as we optimistically assume the input collection has no Collection
		// The actual needed capacity is at most the depth of nesting
		Deque<Iterator<E>> nested = new ArrayDeque<>(1);
		nested.add((Iterator<E>) flatMapper.apply(collection).iterator());

		// In many case, there is nothing to unnest
		// ImmutableList.Builder<E> output = ImmutableList.builderWithExpectedSize(sizeFunction.applyAsInt(collection));
		// Not ImmutableList as we may receive null
		List<E> output = new ArrayList<>(sizeFunction.applyAsInt(collection));

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

		// return output.build();
		return Collections.unmodifiableList(output);
	}

	/**
	 * 
	 * @param c
	 * @return the first item of input {@link Collection}
	 */
	public static Object getFirst(Collection<?> c) {
		if (c.isEmpty()) {
			throw new IllegalArgumentException("Can not .getFirst due to emptyness");
		}
		if (c instanceof List<?> list) {
			return list.getFirst();
		} else {
			return c.iterator().next();
		}
	}

	// eu.solven.pepper.collection.CartesianProductHelper.cartesianProductSize(List<? extends Set<?>>)
	public static BigInteger cartesianProductSize(List<? extends Collection<?>> listOfSets) {
		if (listOfSets.isEmpty()) {
			return BigInteger.ZERO;
		} else {
			return listOfSets.stream()
					.map(c -> BigInteger.valueOf(c.size()))
					.reduce(BigInteger.ONE, BigInteger::multiply);
		}
	}

	/**
	 * Merge input Collections into an {@link ImmutableSet}, skipping `ImmutableSet.builder()` if possible as
	 * `ImmutableSet.copyOf` can be a no-op.
	 * 
	 * @param <T>
	 * @param left
	 * @param right
	 * @return
	 */
	public static <T> ImmutableSet<T> copyOfSets(Collection<? extends T> left, Collection<? extends T> right) {
		if (left.isEmpty()) {
			return ImmutableSet.copyOf(right);
		} else if (right.isEmpty()) {
			return ImmutableSet.copyOf(left);
		} else {
			return ImmutableSet.<T>builder().addAll(left).addAll(right).build();
		}
	}

	@SuppressWarnings("PMD.LooseCoupling")
	public static void trimToSize(Collection<?> collection) {
		if (collection instanceof ArrayList arrayList) {
			arrayList.trimToSize();
		}
	}
}
