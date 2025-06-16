package eu.solven.adhoc.table.duckdb.worldcup;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.example.worldcup.EventAggregation;

public class TestEventAggregation {
	EventAggregation agg = new EventAggregation();

	@Test
	public void testParse() {
		Assertions.assertThat(agg.wrap("G20'").getT()).hasToString("PlayersEvents(typeToMinuteToCounts={G={20=1}})");
	}
}
