package eu.solven.adhoc.map;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.table.transcoder.value.IColumnValueTranscoder;
import eu.solven.adhoc.util.AdhocFactoriesUnsafe;

public class TestAdhocMapHelpers {
	@Test
	public void testFromMapIdentity() {
		ISliceFactory sliceFactory = AdhocFactoriesUnsafe.factories.getSliceFactory();

		IAdhocMap original = sliceFactory.newMapBuilder().put("c", "v").build();
		ITabularRecord originalRecord =
				TabularRecordOverMaps.builder().groupBy(original.asSlice()).aggregate("a", 123L).build();

		ITabularRecord transcoded = originalRecord.transcode(new IColumnValueTranscoder() {

			@Override
			public Object transcodeValue(String column, Object value) {
				return value;
			}

			@Override
			public Set<String> mayTranscode(Set<String> columns) {
				return Set.of();
			}
		});

		Assertions.assertThat(transcoded).isNotSameAs(originalRecord).isEqualTo(originalRecord);
		Assertions.assertThat((Map) transcoded.getGroupBys().asAdhocMap()).isSameAs(original);
	}
}
