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
package eu.solven.adhoc.encoding.dictionary;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ASliceFactory;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.MapOverIntFunction;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.adhoc.util.immutable.ImmutableHelpers;
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
 * Each row are independent (contrary to {@link ColumnarSliceFactory}) and relies on an `int[]` of dictionarized
 * indexes.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Deprecated(since = "Prefer ColumnarSliceFactory")
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

	@Override
	public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotSequenced(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder()
				.factory(this)
				.dictionaryFactory(dictionaryFactory)
				.keys(ImmutableHelpers.copyOf(keys))
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
		} else {
			return buildMapNaively(hasEntries);
		}

	}

}
