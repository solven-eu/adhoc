package eu.solven.adhoc.measure.transformator.iterator;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;

/**
 * Relies on {@link MultitypeNavigableColumn} by default, and {@link UnderlyingQueryStepHelpersV0} to merge underlyings.
 * 
 * @author Benoit Lacelle
 */
public class DagBottomUpStrategyV0 implements IDagBottomUpStrategy {

	@Override
	public IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeNavigableColumn.<SliceAsMap>builder().build();
	}

	@Override
	public Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpersV0.distinctSlices(step, underlyings);
	}

}
