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
package eu.solven.adhoc.encoding.page;

import java.util.Collection;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.encoding.column.AdhocColumnUnsafe;
import eu.solven.adhoc.map.IAdhocMap;
import eu.solven.adhoc.map.factory.ASliceFactory;
import eu.solven.adhoc.map.factory.IMapBuilderPreKeys;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.map.factory.MapOverIntFunction;
import eu.solven.adhoc.map.keyset.SequencedSetLikeList;
import eu.solven.adhoc.options.IHasQueryOptions;
import eu.solven.adhoc.util.NotYetImplementedException;
import eu.solven.adhoc.util.immutable.ImmutableHelpers;
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
 * If you want dictionarization without having to create a transient context, you can consider using a
 * {@link DictionarizedSliceFactory}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class ColumnSliceFactory extends ASliceFactory {

	@Default
	@NonNull
	protected final IHasQueryOptions options = IHasQueryOptions.noOption();

	@Default
	@NonNull
	protected final IAppendableTableFactory appendableTableFactory = new ThreadLocalAppendableTableFactory();

	@Default
	@NonNull
	protected final IAppendableTable appendableTable =
			ThreadLocalAppendableTable.builder().capacity(AdhocColumnUnsafe.getPageSize()).build();

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

		protected String peekNextKey() {
			int currentSize;
			if (row == null) {
				currentSize = 0;
			} else {
				currentSize = row.size();
			}

			if (currentSize >= keysLikeList.size()) {
				// We're full
				return null;
			}

			return keysLikeList.getKey(currentSize);
		}

		@Override
		public MapBuilderPreKeys append(Object value) {
			if (row == null) {
				row = pageFactory.nextRow(keysLikeList);
			}

			int currentSize = row.size();
			if (currentSize >= keysLikeList.size()) {
				throw new IllegalStateException("Can not append v=%s as already filled size=%s keys=%s"
						.formatted(value, currentSize, keysLikeList));
			}
			Object normalizedValue = factory.normalizeCoordinate(value);
			row.add(peekNextKey(), normalizedValue);

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

	@Override
	public IMapBuilderPreKeys newMapBuilder(Iterable<? extends String> keys) {
		assert !isNotSequenced(keys) : "Invalid keys: %s".formatted(PepperLogHelper.getObjectAndClass(keys));

		return MapBuilderPreKeys.builder()
				.factory(this)
				.pageFactory(appendableTable)
				.keys(ImmutableHelpers.copyOf(keys))
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
		} else {
			return buildMapNaively(hasEntries);
		}
	}

}
