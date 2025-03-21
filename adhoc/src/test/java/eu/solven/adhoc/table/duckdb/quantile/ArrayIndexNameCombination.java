package eu.solven.adhoc.table.duckdb.quantile;

import eu.solven.adhoc.dag.step.ISliceWithStep;
import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class ArrayIndexNameCombination implements ICombination {
	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		IAdhocFilter filter = slice.asFilter();
		
		
		
		Integer scenarioIndex = (Integer) slice.getRawSliced("scenarioIndex");
		
		
		return ICombination.super.combine(slice, slicedRecord);
	}
}
