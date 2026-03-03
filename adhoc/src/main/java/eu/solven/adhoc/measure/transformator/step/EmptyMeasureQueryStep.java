package eu.solven.adhoc.measure.transformator.step;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.data.column.Cuboid;
import eu.solven.adhoc.data.column.ICuboid;
import eu.solven.adhoc.engine.step.CubeQueryStep;

public class EmptyMeasureQueryStep implements ITransformatorQueryStep {

	@Override
	public List<CubeQueryStep> getUnderlyingSteps() {
		return ImmutableList.of();
	}

	@Override
	public ICuboid produceOutputColumn(List<? extends ICuboid> underlyings) {
		if (!underlyings.isEmpty()) {
			throw new IllegalArgumentException("Should not be called as we do not have any underlyings. Received %s"
					.formatted(underlyings.size()));
		}
		return Cuboid.empty();
	}

}
