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
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.PermutedArrayList;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * In Adhoc context, we expect to build many {@link Map}-like objects, with same keySet. This is due to the fact we
 * process multiple rows with the same {@link IAdhocGroupBy}.
 *
 * @author Benoit Lacelle
 */
@SuperBuilder
public class StandardSliceFactory extends ASliceFactory {

	/**
	 * A {@link Map} based on {@link SequencedSetLikeList} and values as a {@link List}.
	 *
	 * @author Benoit Lacelle
	 */
	public static class MapOverLists extends AbstractAdhocMap {

		@NonNull
		final ImmutableList<Object> unorderedValues;

		@Builder
		public MapOverLists(ISliceFactory factory, SequencedSetLikeList keys, ImmutableList<Object> unorderedValues) {
			super(factory, keys);
			this.unorderedValues = unorderedValues;
		}

		@Override
		protected Object getUnorderedValue(int index) {
			return unorderedValues.get(index);
		}

		@Override
		protected List<Object> orderedValues() {
			return PermutedArrayList.builder()
					.size(unorderedValues.size())
					.unorderedValues(unorderedValues::get)
					.reordering(keys::unorderedIndex)
					.build();
		}
	}

	/**
	 * A {@link IHasEntries} in which keys are provided initially, and values are received in a later phase in the same
	 * order in the initial keySet.
	 * <p>
	 * To be used when the keySet is known in advance.
	 *
	 * @author Benoit Lacelle
	 */
	@Builder
	public static class MapBuilderPreKeys implements IMapBuilderPreKeys, IHasEntries {
		@NonNull
		StandardSliceFactory factory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Getter
		Collection<? extends String> keys;

		ImmutableList.Builder<Object> values;

		@Override
		public MapBuilderPreKeys append(Object value) {
			if (values == null) {
				values = ImmutableList.builderWithExpectedSize(keys.size());
			}
			Object v = factory.normalizeCoordinate(value);
			values.add(v);

			return this;
		}

		@Override
		public Collection<?> getValues() {
			if (values == null) {
				return ImmutableList.of();
			} else {
				return values.build();
			}
		}

		@Override
		public IAdhocMap build() {
			return factory.buildMap(this);
		}
	}

	/**
	 * A {@link IHasEntries} in which keys are provided with their value.
	 * <p>
	 * To be used when the keySet is not known in advance.
	 *
	 * @author Benoit Lacelle
	 */
	@Builder
	public static class MapBuilderThroughKeys implements IMapBuilderThroughKeys, IHasEntries {
		@NonNull
		StandardSliceFactory factory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Default
		ImmutableList.Builder<String> keys = ImmutableList.builder();

		@Default
		ImmutableList.Builder<Object> values = ImmutableList.builder();

		@Override
		public MapBuilderThroughKeys put(String key, Object value) {
			keys.add(key);
			Object normalizedValue = factory.normalizeCoordinate(value);
			values.add(normalizedValue);

			return this;
		}

		@Override
		public Collection<? extends String> getKeys() {
			return keys.build();
		}

		@Override
		public Collection<?> getValues() {
			return values.build();
		}

		@Override
		public IAdhocMap build() {
			return factory.buildMap(this);
		}
	}

	@Override
	public MapBuilderThroughKeys newMapBuilder() {
		return MapBuilderThroughKeys.builder().factory(this).build();
	}

	@Override
	public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotOrdered(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder().factory(this).keys(ImmutableList.copyOf(keys)).build();
	}

	@Override
	public IAdhocMap buildMap(IHasEntries hasEntries) {
		Collection<? extends String> keys = hasEntries.getKeys();
		Collection<?> values = hasEntries.getValues();

		if (keys.size() != values.size()) {
			throw new IllegalArgumentException(
					"keys size (%s) differs from values size (%s)".formatted(keys.size(), values.size()));
		}

		return MapOverLists.builder()
				.factory(this)
				.keys(internKeyset(keys))
				.unorderedValues(ImmutableList.copyOf(values))
				.build();
	}
}
