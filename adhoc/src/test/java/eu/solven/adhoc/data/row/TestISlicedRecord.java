package eu.solven.adhoc.data.row;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.measure.transformator.iterator.SlicedRecordFromArray;

public class TestISlicedRecord {
	ISlicedRecord slicedRecord = SlicedRecordFromArray.builder().measure("m1").measure("m2").build();

	@Test
	public void testAsList() {
		Assertions.assertThat((List) slicedRecord.asList()).containsExactly("m1", "m2");
	}

	@Test
	public void testIntoArray_shorter() {
		Object[] array = new Object[1];
		slicedRecord.intoArray(array);
		Assertions.assertThat(array).containsExactly("m1");
	}

	@Test
	public void testIntoArray_longer() {
		Object[] array = new Object[3];
		slicedRecord.intoArray(array);
		Assertions.assertThat(array).containsExactly("m1", "m2", null);
	}
}
