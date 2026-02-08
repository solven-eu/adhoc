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

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.compression.page.IAppendableTable;
import eu.solven.adhoc.compression.page.IAppendableTableFactory;
import eu.solven.adhoc.compression.page.ITableRowRead;
import eu.solven.adhoc.compression.page.ITableRowWrite;
import eu.solven.adhoc.compression.page.ThreadLocalAppendableTable;
import eu.solven.adhoc.compression.page.ThreadLocalAppendableTableFactory;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.ICoordinateNormalizer;
import eu.solven.adhoc.map.factory.ASliceFactory;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.IMapBuilderThroughKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.MapOverIntFunction;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.query.cube.IHasQueryOptions;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

/**
 * A {@link ISliceFactory} which enable columnar storage of created {@link IAdhocMap}. This requires to be
 * contextualized (e.g. within a given query), to prevent leaking memory (e.g. we do not want to mix the slices from
 * different queries in the same columns).
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class ColumnarSliceFactory extends ASliceFactory {

	@Default
	@NonNull
	protected final IHasQueryOptions options = IHasQueryOptions.noOption();

	@Default
	@NonNull
	protected final IAppendableTableFactory appendableTableFactory = new ThreadLocalAppendableTableFactory();

	@Default
	@NonNull
	protected final IAppendableTable appendableTable =
			ThreadLocalAppendableTable.builder().capacity(AdhocUnsafe.getPageSize()).build();

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

		// Remember the ordered keys, as we expect to receive values in the same order
		@NonNull
		protected final SequencedSetLikeList keysLikeList;

		@NonNull
		protected final IAppendableTable pageFactory;

		protected ITableRowWrite row;

		@Override
		public Collection<? extends String> getKeys() {
			return keysLikeList;
		}

		@Override
		public MapBuilderPreKeys append(Object value) {
			if (row == null) {
				row = pageFactory.nextRow(keysLikeList);
			}
			Object normalizedValue = factory.normalizeCoordinate(value);
			row.add(keysLikeList.getKey(row.size()), normalizedValue);

			return this;
		}

		@Override
		public Collection<?> getValues() {
			if (row == null) {
				return ImmutableList.of();
			} else {
				throw new NotYetImplementedException("Undictionarize");
			}
		}

		public ITableRowWrite getDictionarizedValues() {
			if (row == null) {
				return ITableRowWrite.empty();
			} else {
				return row;
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
	@Deprecated
	public static class MapBuilderThroughKeys implements IMapBuilderThroughKeys, IHasEntries {
		@NonNull
		ISliceFactory factory;

		@NonNull
		IAppendableTable pageFactory;

		// Remember the ordered keys, as we expect to receive values in the same order
		@Default
		ImmutableList.Builder<String> keys = ImmutableList.builder();

		ITableRowWrite row;

		@Override
		public MapBuilderThroughKeys put(String key, Object value) {
			if (row == null) {
				row = pageFactory.nextRow();
			}

			keys.add(key);
			Object normalizedValue = ((ICoordinateNormalizer) factory).normalizeCoordinate(value);
			row.add(key, normalizedValue);

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
		return MapBuilderThroughKeys.builder().factory(this).pageFactory(appendableTable).build();
	}

	@Override
	public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotOrdered(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder()
				.factory(this)
				.pageFactory(appendableTable)
				.keys(ImmutableList.copyOf(keys))
				.build();
	}

	@Override
	public IAdhocMap buildMap(IHasEntries hasEntries) {
		if (hasEntries instanceof MapBuilderPreKeys preKeys) {
			ITableRowWrite values = preKeys.getDictionarizedValues();

			ITableRowRead frozen = values.freeze();

			return MapOverIntFunction.builder()
					.factory(this)
					.keys(preKeys.keysLikeList)
					.unorderedValues(frozen::readValue)
					.build();
		} else if (hasEntries instanceof MapBuilderThroughKeys throughKeys) {
			Collection<? extends String> keys = throughKeys.getKeys();

			if (throughKeys.row == null) {
				return SliceAsMap.grandTotal().asAdhocMap();
			}
			ITableRowRead frozen = throughKeys.row.freeze();

			SequencedSetLikeList keyLikeList = internKeyset(keys);
			return MapOverIntFunction.builder()
					.factory(this)
					.keys(keyLikeList)
					.unorderedValues(frozen::readValue)
					.build();
		} else {
			return buildMapNaively(hasEntries);
		}
	}

}
