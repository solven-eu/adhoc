package eu.solven.adhoc.measure.combination;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.transformator.ICombinationBinding;

/**
 * Used for {@link ICombination} able to generate a {@link ICombinationBinding}. It may help achieving better CPU/Heap
 * performances.
 * 
 * @author Benoit Lacelle
 */
public interface IBindableCombination {

	ICombinationBinding bind(int nbUnderlyings);

	IValueProvider combine(ICombinationBinding binding, ISliceWithStep slice, ISlicedRecord slicedRecord);

}
