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
			Object raw = getVectorValue(vectorIndex, rowIndex);
			if (raw != null) {
				builder.appendAggregate(aggregates.get(i), ArrowReflection.convertValue(raw));
			}
		}

		for (int i = 0; i < columns.size(); i++, vectorIndex++) {
			Object raw = getVectorValue(vectorIndex, rowIndex);
			builder.appendGroupBy(ArrowReflection.convertValue(raw));
		}

		return builder.build();
	}

	private Object getVectorValue(int vectorIndex, int rowIndex) {
		try {
			return ArrowReflection.GET_OBJECT.invoke(currentVectors.get(vectorIndex), rowIndex);
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