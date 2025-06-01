package eu.solven.adhoc.data.column;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.slice.SliceAsMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestConstantMaskMultitypeColumn {
    @Test
    public void testGetValue() {
        Map<String, ?> mask = Map.of("k2", "v2");

        MultitypeHashColumn<SliceAsMap> withoutMask = MultitypeHashColumn.<SliceAsMap>builder().build();

        withoutMask.set(SliceAsMap.fromMap(Map.of())).onLong(123);

        ConstantMaskMultitypeColumn withMask = ConstantMaskMultitypeColumn.builder().masked(withoutMask).masks(mask).build();

        Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of())))).isEqualTo(null);
        Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k2", "v2"))))).isEqualTo(123L);
    }

    @Test
    public void testConflictingColumns() {
        Map<String, ?> mask = Map.of("k", "v2");

        MultitypeHashColumn<SliceAsMap> withoutMask = MultitypeHashColumn.<SliceAsMap>builder().build();

        withoutMask.set(SliceAsMap.fromMap(Map.of("k", "v1"))).onLong(123);

        ConstantMaskMultitypeColumn withMask = ConstantMaskMultitypeColumn.builder().masked(withoutMask).masks(mask).build();

        Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of())))).isEqualTo(null);
        Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v1"))))).isEqualTo(null);
        Assertions.assertThat(IValueProvider.getValue(withMask.onValue(SliceAsMap.fromMap(Map.of("k", "v2"))))).isEqualTo(null);

        Assertions.assertThatThrownBy(() -> withMask.stream().toList()).isInstanceOf(IllegalArgumentException.class);
    }
}
