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
package eu.solven.adhoc.compression.page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import eu.solven.adhoc.compression.column.IAppendableColumn;
import eu.solven.adhoc.compression.column.IAppendableColumnFactory;
import eu.solven.adhoc.compression.column.ObjectArrayColumnsFactory;
import eu.solven.adhoc.compression.column.freezer.AdhocFreezingUnsafe;
import eu.solven.adhoc.compression.column.freezer.IFreezingStrategy;
import eu.solven.adhoc.compression.column.freezer.SynchronousFreezingStrategy;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A page in a structure representing a table with given columns. It has fixed capacity.
 * 
 * It is not thread-safe. Especially due to lack of synchronization between `columns` and `columnNames`.
 *
 * It has thread-safety requirements when being read-only. Typically, `TablePageRow.freeze` must be thread-safe: it
 * implies the freezing process may be executed by a different thread.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
@NotThreadSafe
public class AppendableTablePage implements IAppendableTablePage {

	@Default
	@NonNull
	final IAppendableColumnFactory columnsFactory = ObjectArrayColumnsFactory.builder().build();

	@NonNull
	@Default
	final IFreezingStrategy freezer =
			SynchronousFreezingStrategy.builder().freezersWithContext(AdhocFreezingUnsafe.getFreezers()).build();

	@Default
	final int capacity = AdhocUnsafe.getPageSize();

	/**
	 * The index of the next polled row. -1 if this page is full.
	 */
	final AtomicInteger nextRow = new AtomicInteger();

	final List<String> columnNames = new ArrayList<>();

	// AtomicReference to be turned into null when freezing
	final AtomicReference<List<IAppendableColumn>> columnsWrite = new AtomicReference<>(new ArrayList<>());
	final AtomicReference<List<? extends IReadableColumn>> columnsRead = new AtomicReference<>(columnsWrite.get());

	/**
	 * If true, the last row has been polled and may or may not be frozen
	 */
	final AtomicBoolean isLastRowPolled = new AtomicBoolean();
	/**
	 * If true, the last row has been frozen: this page is now read-only
	 */
	// final AtomicBoolean isLastRowFrozen = new AtomicBoolean();

	final Thread creationThread = Thread.currentThread();

	/**
	 * An {@link ITableRowWrite} being written.
	 */
	protected final class TablePageRow implements ITableRowWrite {
		private final AtomicInteger columnIndex;
		private final int rowIndex;

		private TablePageRow(AtomicInteger columnIndex, int rowIndex) {
			this.columnIndex = columnIndex;
			this.rowIndex = rowIndex;
		}

		@Override
		public int size() {
			return columnIndex.get();
		}

		@Override
		public int add(String key, Object normalizedValue) {
			int currentColumnIndex = columnIndex.getAndIncrement();

			if (currentColumnIndex >= columnNames.size()) {
				// Register a new additional column
				columnNames.add(key);
				columnsWrite.get().add(makeColumn(key));
			} else {
				String expectedColumnName = columnNames.get(currentColumnIndex);
				if (!key.equals(expectedColumnName)) {
					log.warn("Mis-ordered columns %s != %s".formatted(key, expectedColumnName));
					if (AdhocUnsafe.isFailFast()) {
						throw new IllegalStateException("%s != %s".formatted(key, expectedColumnName));
					} else {
						currentColumnIndex = columnNames.indexOf(key);

						if (currentColumnIndex < 0) {
							currentColumnIndex = columnNames.size();
							columnNames.add(key);
							columnsWrite.get().add(makeColumn(key));
						}
					}
				}
			}

			columnsWrite.get().get(currentColumnIndex).append(normalizedValue);

			return currentColumnIndex;
		}

		@Override
		public ITableRowRead freeze() {
			if (columnNames.size() != size()) {
				throw new IllegalArgumentException(
						"keys size (%s) differs from values size (%s)".formatted(columnNames.size(), size()));
			}

			if (isLastRowPolled.get()) {
				final List<IReadableColumn> columnsReadLocal = new ArrayList<>();

				// Convert from IAppendableColumns to IReadableColumns
				AppendableTablePage.this.columnsWrite.get()
						.stream()
						.map(c -> c.freeze(freezer))
						.forEach(columnsReadLocal::add);

				// isLastRowFrozen.set(true);

				// Remove reference to IAppendableColumns
				// We set the readColumn to workaround race-conditions
				AppendableTablePage.this.columnsRead.set(columnsReadLocal);

				// nullify to enable GC of write columns
				AppendableTablePage.this.columnsWrite.set(null);
			}

			return new TablePageRowRead(size(), rowIndex);
		}

		@Override
		public String toString() {
			return new TablePageRowRead(size(), rowIndex).toString();
		}
	}

	/**
	 * A read-only {@link ITableRowRead}.
	 */
	protected final class TablePageRowRead implements ITableRowRead {
		private final int columnSize;
		private final int rowIndex;

		private TablePageRowRead(int columnSize, int rowIndex) {
			this.columnSize = columnSize;
			this.rowIndex = rowIndex;
		}

		@Override
		public int size() {
			return columnSize;
		}

		@Override
		public Object readValue(int columnIndex) {
			IReadableColumn column = columnsRead.get().get(columnIndex);
			return column.readValue(rowIndex);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			int size = size();
			for (int i = 0; i < size; i++) {
				if (i > 0) {
					sb.append(", ");
				}

				sb.append(columnNames.get(i)).append('=').append(readValue(i));

			}

			return sb.toString();
		}
	}

	public int pollNextRowIndex() {
		return nextRow.getAndAccumulate(1, (l, r) -> {
			if (l < 0) {
				// We already detected the page is full
				return l;
			}
			int polledNextRow = l + r;
			if (polledNextRow >= capacity) {
				isLastRowPolled.set(true);

				// Can not reserve requested rows
				return -1;
			}

			// register the requested rows
			return polledNextRow;
		});
	}

	@Override
	public ITableRowWrite pollNextRow() {
		int rowIndex = pollNextRowIndex();
		if (rowIndex < 0) {
			return null;
		}

		AtomicInteger columnIndex = new AtomicInteger();

		checkThreadSafety();

		return new TablePageRow(columnIndex, rowIndex);
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	protected void checkThreadSafety() {
		Thread currentThread = Thread.currentThread();
		if (creationThread != currentThread) {
			throw new IllegalStateException(
					"Concurrency issue created=%s used=%s".formatted(creationThread, currentThread));
		}
	}

	protected IAppendableColumn makeColumn(String key) {
		return columnsFactory.makeColumn(key, capacity);
	}
}
