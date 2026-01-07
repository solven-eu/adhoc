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
package eu.solven.adhoc.dictionary.page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import eu.solven.adhoc.dictionary.IAppendableColumnFactory;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A page in a structure representing a table with given columns. It has fixed capacity.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
@Builder
public class AppendableTablePage implements IAppendableTablePage {

	@Default
	@NonNull
	final IAppendableColumnFactory columnsFactory = new ObjectArrayColumnsFactory();

	@Default
	final int capacity = AdhocUnsafe.getPageSize();

	final AtomicInteger nextRow = new AtomicInteger();

	final List<String> columnNames = new ArrayList<>();

	final List<IAppendableColumn> columns = new ArrayList<>();
	final List<IReadableColumn> columnsRead = new ArrayList<>();

	final AtomicBoolean isLastRowPolled = new AtomicBoolean();
	final AtomicBoolean isLastRowFrozen = new AtomicBoolean();

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
				columns.add(makeColumn(key));
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
							columns.add(makeColumn(key));
						}
					}
				}
			}

			columns.get(currentColumnIndex).append(normalizedValue);

			return currentColumnIndex;
		}

		@Override
		public ITableRowRead freeze() {
			if (columnNames.size() != size()) {
				throw new IllegalArgumentException(
						"keys size (%s) differs from values size (%s)".formatted(columnNames.size(), size()));
			}

			if (isLastRowPolled.get()) {
				isLastRowFrozen.set(true);

				// Convert from IAppendableColumns to IReadableColumns
				AppendableTablePage.this.columns.stream()
						.map(IAppendableColumn::freeze)
						.forEach(AppendableTablePage.this.columnsRead::add);
				// Remove reference to IAppendableColumns
				AppendableTablePage.this.columns.clear();
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
			IReadableColumn column;
			if (columns.isEmpty()) {
				column = columnsRead.get(columnIndex);
			} else {
				column = columns.get(columnIndex);
			}
			return column.readValue(rowIndex);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < size(); i++) {
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
				// Can not reserve requested rows
				return -1;
			}

			if (polledNextRow == capacity) {
				isLastRowPolled.set(true);
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

		return new TablePageRow(columnIndex, rowIndex);
	}

	protected IAppendableColumn makeColumn(String key) {
		return columnsFactory.makeColumn(key, capacity);
	}
}
