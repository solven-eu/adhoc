package eu.solven.adhoc.dictionary.page;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;
import lombok.experimental.UtilityClass;

/**
 * Helpers around {@link ITableRowRead}
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class TableRowReadHelpers {
	private static final ITableRowRead EMPTY_ROW_READ = new ITableRowRead() {

		@Override
		public int size() {
			return 0;
		}

		@Override
		public Object readValue(int columnIndex) {
			return null;
		}

		@Override
		public String toString() {
			return "empty";
		}

		@Override
		public int hashCode() {
			return ImmutableList.of().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj instanceof ITableRowRead otherRow) {
				return otherRow.size() == 0;
			} else {
				return false;
			}
		}
	};

	private static final ITableRowWrite EMPTY_ROW_WRITE = new ITableRowWrite() {

		@Override
		public int size() {
			return 0;
		}

		@Override
		public ITableRowRead freeze() {
			return ITableRowRead.empty();
		}

		@Override
		public int add(String key, Object normalizedValue) {
			throw new UnsupportedAsImmutableException(key + "=" + normalizedValue);
		}

		@Override
		public String toString() {
			return "empty";
		}

		@Override
		public int hashCode() {
			return ImmutableList.of().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj instanceof ITableRowRead otherRow) {
				return otherRow.size() == 0;
			} else {
				return false;
			}
		}
	};

	static ITableRowRead empty() {
		return EMPTY_ROW_READ;
	}

	static ITableRowWrite emptyRow() {
		return EMPTY_ROW_WRITE;
	}

}
