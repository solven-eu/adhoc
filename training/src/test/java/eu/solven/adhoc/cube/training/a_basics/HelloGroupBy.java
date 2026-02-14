package eu.solven.adhoc.cube.training.a_basics;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.query.cube.IAdhocGroupBy;
import eu.solven.adhoc.query.groupby.GroupByColumns;

/**
 * A GROUP BY is a typical OLAP operation, when data are aggregated at given granularity. The granularity is defined by
 * expressing columns.
 * 
 * If no column is expressed, we're doing a grandTotal. The query will return a single slice: `Map.of()` but the cube
 * might have been filtered given a `WHERE` clause though a ISliceFilter.
 * 
 * @author Benoit Lacelle
 */
public class HelloGroupBy {
	@Test
	public void helloGroupBy() {
		IAdhocGroupBy groupBy = GroupByColumns.named("color", "ccy");

		Assertions.assertThat(groupBy.getGroupedByColumns()).containsExactly("ccy", "color");
		Assertions.assertThat(groupBy.getNameToColumn().get("color")).isEqualTo(ReferencedColumn.ref("color"));
		Assertions.assertThat(groupBy.getNameToColumn().get("ccy")).isEqualTo(ReferencedColumn.ref("ccy"));
	}
}
