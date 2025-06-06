package eu.solven.adhoc.measure.transformator.iterator;

import java.util.List;
import java.util.stream.Stream;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Shiftor;

/**
 * This `v1` improves `v0` by relying on {@link MultitypeNavigableElseHashColumn}, which can handle a large number of
 * slices through a navigable structure, and keep the leftovers (expected to be received at the end) into a hash
 * structure.
 * 
 * `distinctSlices` will returns a {@link Stream} by merging in a first pass the slices from navigable structure, and
 * hashed-slices in a second pass.
 * 
 * It is expected to give better results on measure having a {@link Dispatchor} or {@link Shiftor} underlying: these
 * relies on a hash-column, but a measure depending on them may also receive underlyings with a navigable-column: we
 * should give priority to navigable ordering, and process hashes as left-overs. It would also give better result as it
 * enables switching automatically to a hash-column, instead of doing many insertions at random index if we stick to a
 * navigable-column.
 * 
 * @author Benoit Lacelle
 */
public class DagBottomUpStrategyV1 implements IDagBottomUpStrategy {

	@Override
	public IMultitypeColumnFastGet<SliceAsMap> makeStorage() {
		return MultitypeNavigableElseHashColumn.<SliceAsMap>builder().build();
	}

	@Override
	public Stream<SliceAndMeasures> distinctSlices(CubeQueryStep step, List<? extends ISliceToValue> underlyings) {
		return UnderlyingQueryStepHelpersV1.distinctSlices(step, underlyings);
	}

}
