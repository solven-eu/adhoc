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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.map.ICoordinateNormalizer;
import eu.solven.adhoc.map.StandardCoordinateNormalizer;
import eu.solven.adhoc.map.keyset.SequencedSetUnsafe;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.util.IHasCache;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;

/**
 * In Adhoc context, we expect to build many {@link Map}-like objects, with same keySet. This is due to the fact we
 * process multiple rows with the same {@link IGroupBy}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
public abstract class ASliceFactory implements ISliceFactory, ICoordinateNormalizer, IHasCache {
	// Used to prevent the following pattern: `.newMapBuilder(Set.of("a",
	// "b")).append("a1").append("b1")` as the order
	// of the Set is not consistent with the input array
	private static final Set<Class<? extends Set>> NOT_SEQUENCED_CLASSES;

	static {
		ImmutableSet.Builder<Class<? extends Set>> builder = ImmutableSet.builder();

		// java.util.ImmutableCollections.Set12.Set12(E)
		builder.add(Set.of("a").getClass());
		// java.util.ImmutableCollections.SetN.SetN(E...)
		builder.add(Set.of("a", "b", "c").getClass());

		builder.add(HashSet.class);

		NOT_SEQUENCED_CLASSES = builder.build();
	}

	@Default
	final ICoordinateNormalizer valueNormalizer = new StandardCoordinateNormalizer();

	@Override
	public void invalidateAll() {
		SequencedSetUnsafe.invalidateAll();
	}

	@Override
	public Object normalizeCoordinate(Object raw) {
		return valueNormalizer.normalizeCoordinate(raw);
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
	protected boolean isNotSequenced(Iterable<? extends String> set) {
		if (set instanceof Collection<?> c && c.isEmpty()) {
			return false;
		}

		if (NOT_SEQUENCED_CLASSES.contains(set.getClass())) {
			return true;
		}

		// Assume other Set are ordered
		return false;
	}
}
