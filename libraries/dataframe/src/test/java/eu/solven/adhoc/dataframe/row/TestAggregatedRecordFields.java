package eu.solven.adhoc.dataframe.row;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class TestAggregatedRecordFields {
	@Test
	public void hashcodeEquals() {
		EqualsVerifier.forClass(AggregatedRecordFields.class).withIgnoredFields("allColumns").verify();
	}

	@Test
	public void allColumns() {
		AggregatedRecordFields fields = AggregatedRecordFields.builder().column("a").leftover("b").build();
		Assertions.assertThat(fields.getAllColumns()).containsExactly("a", "b");
	}
}
