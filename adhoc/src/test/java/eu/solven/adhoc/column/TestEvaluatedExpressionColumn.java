package eu.solven.adhoc.column;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.solven.adhoc.data.row.ITabularRecord;
import eu.solven.adhoc.data.row.TabularRecordOverMaps;
import eu.solven.adhoc.data.tabular.TestMapBasedTabularView;
import nl.jqno.equalsverifier.EqualsVerifier;

public class TestEvaluatedExpressionColumn {

	@Test
	public void testHashcodeEquals() {
		EqualsVerifier.forClass(EvaluatedExpressionColumn.class).verify();
	}

	@Test
	public void testJackson() throws JsonProcessingException {
		EvaluatedExpressionColumn column =
				EvaluatedExpressionColumn.builder().name("someColumn").expression("a + b").build();
		String asString = TestMapBasedTabularView.verifyJackson(IAdhocColumn.class, column);

		Assertions.assertThat(asString).isEqualTo("""
				{
				  "type" : ".EvaluatedExpressionColumn",
				  "name" : "someColumn",
				  "type" : "java.lang.Object",
				  "expression" : "a + b"
				}""");
	}

	@Test
	public void testEvaluate() throws JsonProcessingException {
		EvaluatedExpressionColumn column =
				EvaluatedExpressionColumn.builder().name("someColumn").expression("a + \"-\" + b").build();
		// not null
		Assertions
				.assertThat(column
						.computeCoordinate(TabularRecordOverMaps.builder().slice(Map.of("a", "a1", "b", "b1")).build()))
				.isEqualTo("a1-b1");

		// one is null
		Assertions
				.assertThat(column.computeCoordinate(TabularRecordOverMaps.builder().slice(Map.of("a", "a1")).build()))
				.isEqualTo("a1-null");
	}
}
