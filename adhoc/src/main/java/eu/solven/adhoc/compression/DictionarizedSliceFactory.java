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
package eu.solven.adhoc.compression;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.ICoordinateNormalizer;
import eu.solven.adhoc.map.factory.ASliceFactory;
import eu.solven.adhoc.map.factory.AbstractAdhocMap;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.IMapBuilderThroughKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.SequencedSetLikeList;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * A {@link ISliceFactory} which enable dictionarization of created {@link IAdhocMap}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class DictionarizedSliceFactory extends ASliceFactory {

	@Default
	@NonNull
	IDictionarizerFactory dictionaryFactory = new IDictionarizerFactory() {
		final Map<String, IDictionarizer> columnToDic = new ConcurrentHashMap<>();

		@Override
		public IDictionarizer makeDictionarizer(String column) {
			return columnToDic.computeIfAbsent(column, this::newDictionarizer);
		}

		protected IDictionarizer newDictionarizer(String column) {
			return MapDictionarizer.builder().build();
		}
	};

	/**
	 * Represents an {@link IAdhocMap} given an {@link IntFunction} representation dictionarized value.
	 */
	public static class MapOverIntFunction extends AbstractAdhocMap {

		@NonNull
		final IntFunction<Object> sequencedValues;

		@Builder
		public MapOverIntFunction(ISliceFactory factory,
				SequencedSetLikeList keys,
				IntFunction<Object> unorderedValues) {
			super(factory, keys);
			this.sequencedValues = unorderedValues;
		}

		@Override
		protected Object getSequencedValueRaw(int index) {
			return sequencedValues.apply(index);
		}

		@Override
		protected Object getSortedValueRaw(int index) {
			return getSequencedValueRaw(sequencedKeys.unorderedIndex(index));
		}

		@Override
		public IAdhocMap retainAll(Set<String> retainedColumns) {
			RetainedKeySet retainedKeyset = retainKeyset(retainedColumns);

			int[] sequencedIndexes = retainedKeyset.getSequencedIndexes();
			IntFunction<Object> retainedSequencedValues = index -> sequencedValues.apply(sequencedIndexes[index]);

			return new MapOverIntFunction(getFactory(), retainedKeyset.getKeys(), retainedSequencedValues);
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
		protected final ASliceFactory factory;

		@NonNull
		protected final IDictionarizerFactory dictionaryFactory;

		// Remember the ordered keys, as we expect to receive values in the same order
		SequencedSetLikeList keysLikeList;

		IntList values;

		@Override
		public Collection<? extends String> getKeys() {
			return keysLikeList;
		}

		@Override
		public MapBuilderPreKeys append(Object value) {
			if (values == null) {
				values = new IntArrayList(keysLikeList.size());
			}
			Object normalizedValue = factory.normalizeCoordinate(value);
			int dictionarizedValue =
					dictionaryFactory.makeDictionarizer(keysLikeList.getKey(values.size())).toInt(normalizedValue);
			values.add(dictionarizedValue);

			return this;
		}

		@Override
		public Collection<?> getValues() {
			if (values == null) {
				return ImmutableList.of();
			} else {
				throw new NotYetImplementedException("Undictionarize");
			}
		}

		public IntList getDictionarizedValues() {
			if (values == null) {
				return IntArrayList.of();
			} else {
				return values;
			}
		}

		@Override
		public IAdhocMap build() {
			return factory.buildMap(this);
		}

		/**
		 * Lombok @Builder
		 */
		public static class MapBuilderPreKeysBuilder {
			public MapBuilderPreKeysBuilder keys(Collection<? extends String> keys) {
				return keysLikeList(factory.internKeyset(keys));
			}
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
		ISliceFactory factory;

		@NonNull
		IDictionarizerFactory dictionaryFactory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Default
		ImmutableList.Builder<String> keys = ImmutableList.builder();

		@Default
		IntList values = new IntArrayList();

		@Override
		public MapBuilderThroughKeys put(String key, Object value) {
			keys.add(key);
			Object normalizedValue = ((ICoordinateNormalizer) factory).normalizeCoordinate(value);
			int dictionarizedValue = dictionaryFactory.makeDictionarizer(key).toInt(normalizedValue);
			values.add(dictionarizedValue);

			return this;
		}

		@Override
		public Collection<? extends String> getKeys() {
			return keys.build();
		}

		@Override
		public Collection<?> getValues() {
			throw new NotYetImplementedException("Undictionarize");
		}

		@Override
		public IAdhocMap build() {
			return factory.buildMap(this);
		}
	}

	@Override
	public MapBuilderThroughKeys newMapBuilder() {
		return MapBuilderThroughKeys.builder().factory(this).dictionaryFactory(dictionaryFactory).build();
	}

	@Override
	public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotOrdered(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder()
				.factory(this)
				.dictionaryFactory(dictionaryFactory)
				.keys(ImmutableList.copyOf(keys))
				.build();
	}

	@Override
	public IAdhocMap buildMap(IHasEntries hasEntries) {
		if (hasEntries instanceof MapBuilderPreKeys preKeys) {
			IntList values = preKeys.getDictionarizedValues();

			if (preKeys.keysLikeList.size() != values.size()) {
				throw new IllegalArgumentException("keys size (%s) differs from values size (%s)"
						.formatted(preKeys.keysLikeList.size(), values.size()));
			}

			return MapOverIntFunction.builder()
					.factory(this)
					.keys(preKeys.keysLikeList)
					.unorderedValues(i -> dictionaryFactory.makeDictionarizer(preKeys.keysLikeList.getKey(i))
							.fromInt(values.getInt(i)))
					.build();
		} else if (hasEntries instanceof MapBuilderThroughKeys throughKeys) {
			Collection<? extends String> keys = throughKeys.getKeys();
			if (keys.size() != throughKeys.values.size()) {
				throw new IllegalArgumentException("keys size (%s) differs from values size (%s)".formatted(keys.size(),
						throughKeys.values.size()));
			}

			SequencedSetLikeList keyLikeList = internKeyset(keys);
			return MapOverIntFunction.builder()
					.factory(this)
					.keys(keyLikeList)
					.unorderedValues(i -> dictionaryFactory.makeDictionarizer(keyLikeList.getKey(i))
							.fromInt(throughKeys.values.getInt(i)))
					.build();
		} else {
			return buildMapNaively(hasEntries);
		}

	}

}
