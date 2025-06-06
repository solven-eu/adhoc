package eu.solven.adhoc.measure.transformator.iterator;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.transformator.step.ITransformatorQueryStep;

/**
 * Wraps the strategy to store results from {@link ITransformatorQueryStep}, and to merge underlyings
 * {@link ITransformatorQueryStep} results.
 * 
 * @author Benoit Lacelle
 */
public interface IDagBottomUpStrategy {

	/**
	 * 
	 * @return the storage for a {@link ITransformatorQueryStep} output.
	 */
	IMultitypeColumnFastGet<SliceAsMap> makeStorage();

	/**
	 * 
	 * @param step
	 * @param underlyings
	 * @return a {@link Stream} of {@link SliceAndMeasure}, which each {@link SliceAndMeasure} have a distinct
	 *         {@link SliceAsMap}, and the relevant value from underlyings.
	 */
	Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings);

}
