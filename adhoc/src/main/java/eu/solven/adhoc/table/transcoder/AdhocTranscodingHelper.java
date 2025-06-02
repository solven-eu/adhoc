/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table.transcoder;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.query.filter.FilterHelpers;
import eu.solven.adhoc.query.filter.IAdhocFilter;
import eu.solven.adhoc.query.filter.IAndFilter;
import eu.solven.adhoc.query.filter.IColumnFilter;
import eu.solven.adhoc.query.filter.IFilterVisitor;
import eu.solven.adhoc.query.filter.INotFilter;
import eu.solven.adhoc.query.filter.IOrFilter;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps transcoding from one column-model to another. Typically as measures may refer to a given set of columns, while
 * underlying database may have different names for these columns.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@UtilityClass
public class AdhocTranscodingHelper {

	static final AtomicLong COUNT_SUBOPTIMAL = new AtomicLong();

	// TODO Should return original Map is there is no actual transcoding
	public static Map<String, ?> transcodeColumns(ITableReverseTranscoder reverseTranscoder,
			Map<String, ?> underlyingMap) {
		int initialCapacity = reverseTranscoder.estimateQueriedSize(underlyingMap.keySet());
		Map<String, Object> transcoded = new HashMap<>(initialCapacity);

		underlyingMap.forEach((underlyingKey, v) -> {
			Set<String> queriedKeys = reverseTranscoder.queried(underlyingKey);

			if (queriedKeys.isEmpty()) {
				// This output column was not requested, but it has been received. The DB returns unexpected columns?
				// String queriedKey = underlyingKey;
				// insertTranscoded(v, queriedKey, transcoded);
				log.warn("No queried keys found for {}", underlyingKey);
			} else {
				queriedKeys.forEach(queriedKey -> {
					insertTranscoded(v, queriedKey, transcoded);
				});
			}
		});

		if (initialCapacity < transcoded.size()) {
			long count = COUNT_SUBOPTIMAL.incrementAndGet();
			if (Long.bitCount(count) == 1) {
				log.warn("Sub-optimal capacity (capacity={} < reversedSize={})", initialCapacity, transcoded.size());
			} else {
				log.debug("Sub-optimal capacity (capacity={} < reversedSize={})", initialCapacity, transcoded.size());
			}
		}

		return transcoded;
	}

	private static void insertTranscoded(Object v, String queriedKey, Map<String, Object> transcoded) {
		Object replaced = transcoded.put(queriedKey, v);

		if (replaced != null && !replaced.equals(v)) {
			// BEWARE Should we drop a static method as this may be customized?
			log.warn(
					"Transcoding led to an ambiguity as multiple underlyingKeys has queriedKey={} mapping to values {} and {}",
					queriedKey,
					replaced,
					v);
		}
	}

	/**
	 *
	 * @param transcoder
	 * @param notTranscoded
	 * @return a {@link Map} where each value is replaced by the transcoded value.
	 */
	public static Map<String, ?> transcodeValues(IColumnValueTranscoder transcoder, Map<String, ?> notTranscoded) {
		// Store in a transient List, which keeps order while skipping any HashMap cost
		AtomicReference<List<Map.Entry<String, Object>>> columnToTranscodedValue = new AtomicReference<>();

		notTranscoded.forEach((column, rawValue) -> {
			Object transcodedValue = transcoder.transcodeValue(column, rawValue);

			// Register only not trivial mappings
			if (!Objects.equals(rawValue, transcodedValue)) {
				if (columnToTranscodedValue.get() == null) {
					columnToTranscodedValue.set(new ArrayList<>());
				}

				columnToTranscodedValue.get().add(new AbstractMap.SimpleImmutableEntry<>(column, transcodedValue));
			}
		});

		if (columnToTranscodedValue.get() == null) {
			// Not a single transcoding: return original Map
			return notTranscoded;
		}

		// Initialize with notTranscoded values
		Map<String, Object> transcoded = new LinkedHashMap<>(notTranscoded);

		// Replace transcoded values
		columnToTranscodedValue.get().forEach(e -> transcoded.put(e.getKey(), e.getValue()));

		return transcoded;
	}

	/**
	 * 
	 * @param filter
	 * @param input
	 * @return true if the input matches the filter
	 */
	public static boolean match(IAdhocFilter filter, Map<String, ?> input) {
		return match(new IdentityImplicitTranscoder(), filter, input);
	}

	/**
	 * 
	 * @param transcoder
	 * @param filter
	 * @param input
	 * @return true if the input matches the filter, where each column in input is transcoded.
	 */
	public static boolean match(ITableTranscoder transcoder, IAdhocFilter filter, Map<String, ?> input) {
		return FilterHelpers.visit(filter, new IFilterVisitor() {

			@Override
			public boolean testAndOperands(Set<? extends IAdhocFilter> operands) {
				return operands.stream().allMatch(f -> match(transcoder, f, input));
			}

			@Override
			public boolean testOrOperands(Set<? extends IAdhocFilter> operands) {
				return operands.stream().anyMatch(f -> match(transcoder, f, input));
			}

			@Override
			public boolean testColumnOperand(IColumnFilter columnFilter) {
				String underlyingColumn = transcoder.underlyingNonNull(columnFilter.getColumn());
				Object value = input.get(underlyingColumn);

				if (value == null) {
					if (input.containsKey(underlyingColumn)) {
						log.trace("Key to null-ref");
					} else {
						log.trace("Missing key");
						if (columnFilter.isNullIfAbsent()) {
							log.trace("Treat absent as null");
						} else {
							log.trace("Do not treat absent as null, but as missing, hence not matched");
							return false;
						}
					}
				}

				return columnFilter.getValueMatcher().match(value);
			}

			@Override
			public boolean testNegatedOperand(IAdhocFilter negated) {
				return !match(transcoder, negated, input);
			}

		});
	}

	public static boolean match(ITableTranscoder transcoder, IAdhocFilter filter, ITabularRecord input) {
		if (filter.isMatchAll()) {
			return true;
		} else if (filter.isMatchNone()) {
			return false;
		} else if (filter.isAnd() && filter instanceof IAndFilter andFilter) {
			return andFilter.getOperands().stream().allMatch(f -> match(transcoder, f, input));
		} else if (filter.isOr() && filter instanceof IOrFilter orFilter) {
			return orFilter.getOperands().stream().anyMatch(f -> match(transcoder, f, input));
		} else if (filter.isColumnFilter() && filter instanceof IColumnFilter columnFilter) {
			String underlyingColumn = transcoder.underlyingNonNull(columnFilter.getColumn());
			Object value = input.getGroupBy(underlyingColumn);

			if (value == null) {
				if (input.getGroupBys().containsKey(underlyingColumn)) {
					log.trace("Key to null-ref");
				} else {
					log.trace("Missing key");
					if (columnFilter.isNullIfAbsent()) {
						log.trace("Treat absent as null");
					} else {
						log.trace("Do not treat absent as null, but as missing hence not acceptable");
						return false;
					}
				}
			}

			return columnFilter.getValueMatcher().match(value);
		} else if (filter.isNot() && filter instanceof INotFilter notFilter) {
			return !match(transcoder, notFilter.getNegated(), input);
		} else {
			throw new UnsupportedOperationException(PepperLogHelper.getObjectAndClass(filter).toString());
		}
	}
}
