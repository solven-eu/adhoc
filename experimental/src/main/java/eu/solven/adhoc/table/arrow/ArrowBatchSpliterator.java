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
package eu.solven.adhoc.table.arrow;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import eu.solven.adhoc.dataframe.row.ITabularRecord;
import eu.solven.adhoc.dataframe.row.ITabularRecordFactory;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;
import lombok.RequiredArgsConstructor;

/**
 * A {@link Spliterator} that lazily iterates over Arrow record batches produced by an {@code ArrowReader}.
 * 
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
public final class ArrowBatchSpliterator implements Spliterator<ITabularRecord> {

	final Object arrowReader;
	final ITabularRecordFactory factory;
	final QueryPod queryPod;
	final int minSplitRows;

	// Current batch state (populated by loadNextBatch)
	Object currentRoot;
	List<?> currentVectors;
	int currentBatchSize;
	int currentRowIndex;
	boolean exhausted;

	@Override
	public boolean tryAdvance(Consumer<? super ITabularRecord> action) {
		while (currentRowIndex >= currentBatchSize) {
			if (!loadNextBatch()) {
				return false;
			}
		}

		if (queryPod.isCancelled()) {
			throw new CancelledQueryException("Query cancelled during Arrow stream iteration");
		}

		action.accept(buildRecord(currentRowIndex));
		currentRowIndex++;
		return true;
	}

	private boolean loadNextBatch() {
		if (exhausted) {
			return false;
		}
		try {
			boolean hasMore = (boolean) ArrowReflection.LOAD_NEXT_BATCH.invoke(arrowReader);
			if (!hasMore) {
				exhausted = true;
				return false;
			}
			Object root = ArrowReflection.GET_VECTOR_SCHEMA_ROOT.invoke(arrowReader);
			currentRoot = root;
			currentBatchSize = (int) ArrowReflection.GET_ROW_COUNT.invoke(root);
			currentVectors = (List<?>) ArrowReflection.GET_FIELD_VECTORS.invoke(root);
			currentRowIndex = 0;
			return true;
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error loading next Arrow batch", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	private ITabularRecord buildRecord(int rowIndex) {
		return ArrowReflection.buildRecord(currentVectors, rowIndex, factory);
	}

	@Override
	public Spliterator<ITabularRecord> trySplit() {
		// Ensure there is at least one loaded batch to split
		while (currentRowIndex >= currentBatchSize) {
			if (!loadNextBatch()) {
				return null;
			}
		}
		int remaining = currentBatchSize - currentRowIndex;
		if (remaining < minSplitRows) {
			return null;
		}
		int splitSize = remaining / 2;

		// VectorSchemaRoot.slice() shares the underlying ArrowBuf objects via reference counting.
		// When loadNextBatch() later calls vector.clear() on the original root, the ref-count drops
		// by 1 but does not reach zero, so the sliced root's buffers remain valid.
		// The returned ArrowFixedBatchSpliterator must close the sliced root when exhausted.
		// See https://arrow.apache.org/docs/java/vector_schema_root.html
		try {
			Object slicedRoot = ArrowReflection.SLICE.invoke(currentRoot, currentRowIndex, splitSize);
			currentRowIndex += splitSize;
			return new ArrowFixedBatchSpliterator(slicedRoot, splitSize, factory, queryPod, minSplitRows);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error slicing Arrow VectorSchemaRoot", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	@Override
	public long estimateSize() {
		return Long.MAX_VALUE;
	}

	@Override
	public int characteristics() {
		return ORDERED | NONNULL;
	}
}
