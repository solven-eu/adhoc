/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.theta.UpdateSketch;

import eu.solven.adhoc.encoding.column.IAppendableColumn;
import eu.solven.adhoc.encoding.column.IReadableColumn;
import eu.solven.adhoc.encoding.column.ObjectArrayColumn;
import eu.solven.adhoc.encoding.column.freezer.FreezerHelpers;
import eu.solven.adhoc.encoding.column.freezer.IFreezingWithContext;

/**
 * {@link IFreezingWithContext} which will enable dictionarization.
 *
 * @author Benoit Lacelle
 */
public final class DistinctFreezer implements IFreezingWithContext {
	private static final int DISTINCT_FACTOR = 16;

	@Override
	public Optional<IReadableColumn> freeze(IAppendableColumn column, Map<String, Object> freezingContext) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<?> array = arrayColumn.getAsArray();

			int limitForDictionary = array.size() / DISTINCT_FACTOR;
			long countDistinct = countDistinctWithLimit(freezingContext, array, limitForDictionary);

			if (countDistinct <= limitForDictionary) {
				return Optional.of(DictionarizedObjectColumn.fromArray(array));
			} else {
				return Optional.empty();
			}
		} else {
			return Optional.empty();
		}
	}

	long countDistinctWithLimit(Map<String, Object> freezingContext, List<?> array, int limitForDictionary) {
		Set<?> classes = FreezerHelpers.classesWithContext(freezingContext, array);

		return (long) freezingContext.computeIfAbsent("count_distinct_" + limitForDictionary, _ -> {
			if (classes.stream().anyMatch(clazz -> clazz != null && clazz != String.class)) {
				// At least one is neither null nor String
				return cappedDistinctCount(array, limitForDictionary);
			} else {
				// Only String or null
				List<String> arrayString = array.stream().map(String.class::cast).toList();
				return estimateDistinctHLL(arrayString);
			}
		});
	}

	static long cappedDistinctCount(List<?> array, int limitForDictionary) {
		return array.stream()
				.distinct()
				// No need to count distinct all, we just need to know if it is greater or not than some limit
				.limit(limitForDictionary + 1L)
				.count();
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	static long estimateDistinctHLL(List<String> items) {
		// Create HLL sketch (log2m = 12, type HLL_4)
		HllSketch sketch = new HllSketch(12, TgtHllType.HLL_4);

		// Add items to sketch
		for (String s : items) {
			sketch.update(s);
		}

		// Return estimated cardinality
		return (long) sketch.getEstimate();
	}

	@SuppressWarnings("checkstyle:MagicNumber")
	static long estimateDistinctKMV(List<String> items) {
		// Create a Theta sketch with nominal entries = 1024
		UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(1024).build();

		// Add items to the sketch
		for (String s : items) {
			sketch.update(s);
		}

		// Return estimated cardinality
		return (long) sketch.getEstimate();
	}
}