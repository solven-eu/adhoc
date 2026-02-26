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
package eu.solven.adhoc.table.sql.duckdb;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.ITabularRecordFactory;
import eu.solven.adhoc.data.row.TabularRecordBuilder;
import eu.solven.adhoc.engine.cancel.CancelledQueryException;
import eu.solven.adhoc.engine.context.QueryPod;

// -------------------------------------------------------------------------
// Spliterator over Arrow record batches
// -------------------------------------------------------------------------

/**
 * A {@link Spliterator} that lazily iterates over Arrow record batches produced by an {@code ArrowReader}.
 * 
 * @author Benoit Lacelle
 */
public final class ArrowBatchSpliterator implements Spliterator<ITabularRecord> {

	final Object arrowReader;
	final ITabularRecordFactory factory;
	final QueryPod queryPod;

	// Current batch state (populated by loadNextBatch)
	List<?> currentVectors;
	int currentBatchSize;
	int currentRowIndex;
	boolean exhausted;

	public ArrowBatchSpliterator(Object arrowReader, ITabularRecordFactory factory, QueryPod queryPod) {
		this.arrowReader = arrowReader;
		this.factory = factory;
		this.queryPod = queryPod;
	}

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
		TabularRecordBuilder builder = factory.makeTabularRecordBuilder();

		List<String> aggregates = factory.getAggregates();
		List<String> columns = factory.getColumns();

		int vectorIndex = 0;

		for (int i = 0; i < aggregates.size(); i++, vectorIndex++) {
			Object value = getVectorValue(vectorIndex, rowIndex);
			if (value != null) {
				builder.appendAggregate(aggregates.get(i), value);
			}
		}

		for (int i = 0; i < columns.size(); i++, vectorIndex++) {
			builder.appendGroupBy(getVectorValue(vectorIndex, rowIndex));
		}

		return builder.build();
	}

	private Object getVectorValue(int vectorIndex, int rowIndex) {
		Object vector = currentVectors.get(vectorIndex);
		try {
			Object raw = ArrowReflection.GET_OBJECT.invoke(vector, rowIndex);
			return ArrowReflection.convertValue(raw, vector);
		} catch (InvocationTargetException e) {
			throw new IllegalStateException("Error reading Arrow vector value", e.getCause());
		} catch (IllegalAccessException e) {
			throw new IllegalStateException("Unexpected reflection access error", e);
		}
	}

	@Override
	public Spliterator<ITabularRecord> trySplit() {
		// No splitting: Arrow batches are read sequentially
		return null;
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