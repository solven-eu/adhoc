package eu.solven.adhoc.compression.column;

import java.util.List;

import eu.solven.adhoc.compression.dictionary.DictionarizedObjectColumn;
import eu.solven.adhoc.compression.page.IReadableColumn;

public class StandardFreezingStrategy implements IFreezingStrategy {

	@Override
	public IReadableColumn freeze(IAppendableColumn column) {
		if (column instanceof ObjectArrayColumn arrayColumn) {
			List<Object> array = arrayColumn.asArray;
			long countDistinct = array.stream().distinct().count();

			// TODO This computation could be done asynchronously
			if (countDistinct * 16 <= array.size()) {
				return DictionarizedObjectColumn.fromArray(array);
			} else if (array.stream().allMatch(Long.class::isInstance)) {
				long[] primitiveArray = array.stream().mapToLong(Long.class::cast).toArray();
				return LongArrayColumn.builder().asArray(primitiveArray).build();
			} else {
				// TODO wrap in unmodifiable?
				return column;
			}
		} else {
			// TODO wrap in unmodifiable?
			return column;
		}
	}

}
