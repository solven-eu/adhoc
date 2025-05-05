package eu.solven.adhoc.measure.transformator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.IMultitypeColumnFastGet;
import eu.solven.adhoc.data.column.ISliceToValue;
import eu.solven.adhoc.data.column.MultitypeHashColumn;
import eu.solven.adhoc.data.column.SliceToValue;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.iterator.SliceAndMeasures;

public class TestUnderlyingQueryStepHelpers {
	@Test
	public void testDistinctSlices_notOrdered_oneUnderlying() {
		CubeQueryStep queryStep = CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build();
		List<ISliceToValue> underlyings = new ArrayList<>();

		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c1"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c2"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}

		List<SliceAndMeasures> slices = UnderlyingQueryStepHelpers.distinctSlices(queryStep, underlyings).toList();

		Assertions.assertThat(slices).hasSize(3).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c1"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c2"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c3"));
		});
	}

	@Test
	public void testDistinctSlices_notOrdered_twoUnderlying() {
		CubeQueryStep queryStep = CubeQueryStep.builder().measure(Aggregator.countAsterisk()).build();
		List<ISliceToValue> underlyings = new ArrayList<>();

		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c1"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c2"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}
		{
			IMultitypeColumnFastGet<SliceAsMap> column = MultitypeHashColumn.<SliceAsMap>builder().build();
			column.append(SliceAsMap.fromMap(Map.of("c", "c3"))).onLong(123);
			column.append(SliceAsMap.fromMap(Map.of("c", "c5"))).onLong(345);
			column.append(SliceAsMap.fromMap(Map.of("c", "c4"))).onLong(234);
			underlyings.add(SliceToValue.builder().column(column).build());
		}

		List<SliceAndMeasures> slices = UnderlyingQueryStepHelpers.distinctSlices(queryStep, underlyings).toList();

		Assertions.assertThat(slices).hasSize(5).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c1"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c2"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c3"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c4"));
		}).anySatisfy(slice -> {
			Assertions.assertThat(slice.getSlice().getAdhocSliceAsMap().getCoordinates()).isEqualTo(Map.of("c", "c5"));
		});
	}
}
