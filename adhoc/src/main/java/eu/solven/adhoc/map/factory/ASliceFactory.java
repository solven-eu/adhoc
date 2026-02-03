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
package eu.solven.adhoc.map.factory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.ICoordinateNormalizer;
import eu.solven.adhoc.map.StandardCoordinateNormalizer;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;

/**
 * In Adhoc context, we expect to build many {@link Map}-like objects, with same keySet. This is due to the fact we
 * process multiple rows with the same {@link IAdhocGroupBy}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
public abstract class ASliceFactory implements ISliceFactory, ICoordinateNormalizer {
	// Used to prevent the following pattern: `.newMapBuilder(Set.of("a",
	// "b")).append("a1").append("b1")` as the order
	// of the Set is not consistent with the input array
	private static final Set<Class<? extends Set>> NOT_ORDERED_CLASSES;

	static {
		ImmutableSet.Builder<Class<? extends Set>> builder = ImmutableSet.builder();

		// java.util.ImmutableCollections.Set12.Set12(E)
		builder.add(Set.of("a").getClass());
		// java.util.ImmutableCollections.SetN.SetN(E...)
		builder.add(Set.of("a", "b", "c").getClass());

		builder.add(HashSet.class);

		NOT_ORDERED_CLASSES = builder.build();
	}

	final ConcurrentMap<Integer, NavigableSetLikeList> keySetDictionary = new ConcurrentHashMap<>();
	final ConcurrentMap<NavigableSetLikeList, Integer> keySetDictionaryReverse = new ConcurrentHashMap<>();
	final ConcurrentMap<List<String>, SequencedSetLikeList> listToKeyset = new ConcurrentHashMap<>();

	@Default
	final ICoordinateNormalizer valueNormalizer = new StandardCoordinateNormalizer();

	// Supplier as the sliceFactory may be configured lazily
	private static final Supplier<IAdhocMap> EMPTY = Suppliers.memoize(() -> RowSliceFactory.MapOverLists.builder()
			.factory(AdhocFactoriesUnsafe.factories.getSliceFactoryFactory().makeFactory())
			.keys(SequencedSetLikeList.fromSet(Set.of()))
			.sequencedValues(ImmutableList.of())
			.build());

	public static IAdhocMap of() {
		return EMPTY.get();
	}

	@Override
	public Object normalizeCoordinate(Object raw) {
		return valueNormalizer.normalizeCoordinate(raw);
	}

	/**
	 * Describe a {@link Map}-like structure by its keys and its values. The keySet and values can be zipped together
	 * (i.e. iterated concurrently).
	 *
	 * @author Benoit Lacelle
	 */
	public interface IHasEntries {
		Collection<? extends String> getKeys();

		Collection<?> getValues();
	}

	/**
	 * This method is useful to report miss-behaving {@link Set} given we expect proper ordering: the Set may not be
	 * ordered, but one expect it to iterate consistently.
	 * <p>
	 * BEWARE If false, it is not guaranteed the input is ordered .
	 *
	 * @param set
	 * @return true if the input if an ordered {@link Set}
	 */
	protected boolean isNotOrdered(Iterable<? extends String> set) {
		if (set instanceof Collection<?> c && c.isEmpty()) {
			return false;
		}

		if (NOT_ORDERED_CLASSES.contains(set.getClass())) {
			return true;
		}

		// Assume other Set are ordered
		return false;
	}

	@Override
	public SequencedSetLikeList internKeyset(Collection<? extends String> keys) {
		List<? extends String> keysAsList = copyAsList(keys);

		SequencedSetLikeList optExisting = listToKeyset.get(keysAsList);

		if (optExisting != null) {
			return optExisting;
		} else {
			return register(keysAsList);
		}
	}

	@SuppressWarnings("PMD.AvoidSynchronizedStatement")
	protected SequencedSetLikeList register(Collection<? extends String> keys) {
		List<String> keysAsList = copyAsList(keys);

		SequencedSetLikeList sequencedSetLikeList = listToKeyset.computeIfAbsent(keysAsList, k -> {
			return SequencedSetLikeList.fromCollection(keysAsList);
		});

		NavigableSetLikeList setLikeList = sequencedSetLikeList.set;

		synchronized (this) {
			int size = keySetDictionary.size();
			keySetDictionary.put(size, setLikeList);
			keySetDictionaryReverse.put(setLikeList, size);
		}

		return sequencedSetLikeList;
	}

	protected List<String> copyAsList(Collection<? extends String> keys) {
		return ImmutableList.copyOf(keys);
	}

	protected IAdhocMap buildMapNaively(IHasEntries hasEntries) {
		Collection<? extends String> keys = hasEntries.getKeys();
		Collection<?> values = hasEntries.getValues();

		if (keys.size() != values.size()) {
			throw new IllegalArgumentException(
					"keys size (%s) differs from values size (%s)".formatted(keys.size(), values.size()));
		}

		return RowSliceFactory.MapOverLists.builder()
				.factory(this)
				.keys(internKeyset(keys))
				.sequencedValues(ImmutableList.copyOf(values))
				.build();
	}
}
