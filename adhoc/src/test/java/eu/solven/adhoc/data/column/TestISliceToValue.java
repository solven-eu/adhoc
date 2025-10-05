package eu.solven.adhoc.data.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.data.row.slice.SliceAsMap;

public class TestISliceToValue {
	@Test
	public void testGetValue() {
		IMultitypeColumnFastGet<IAdhocSlice> values = MultitypeHashColumn.<IAdhocSlice>builder().capacity(1).build();
		IAdhocSlice slice = SliceAsMap.fromMap(Map.of("a", "a1", "b", "b1"));
		values.append(slice).onLong(123);

		ISliceToValue sliceToValue = SliceToValue.builder().column("a").column("b").values(values).build();

		Assertions.assertThat(ISliceToValue.getValue(sliceToValue, slice)).isEqualTo(123L);
	}
}
