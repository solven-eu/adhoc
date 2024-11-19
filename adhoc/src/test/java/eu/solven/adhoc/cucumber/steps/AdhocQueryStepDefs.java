package eu.solven.adhoc.cucumber.steps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.ITabularView;
import eu.solven.adhoc.database.IAdhocDatabaseWrapper;
import eu.solven.adhoc.database.InMemoryDatabase;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.AdhocQuery.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.slf4j.Slf4j;

/**
 * Basic step-definitions where the {@link IAdhocDatabaseWrapper} is a {@link InMemoryDatabase}.
 * 
 * @author Benoit Lacelle
 *
 */
@Slf4j
public class AdhocQueryStepDefs {

	ADagTest aDagTest = new ADagTest() {

		@Override
		public void feedDb() {

		}
	};

	ITabularView tabularView;
	Throwable t;

	@Before
	public void setUp() {
		aDagTest.wireEvents();
	}

	@Given("Append rows")
	public void appendRows(DataTable data) {
		List<Map<String, String>> maps = data.asMaps();

		maps.forEach(aDagTest.rows::add);
	}

	@Given("Register aggregator name={word} column={word} key={word}")
	public void registerAggregator(String name, String column, String key) {
		aDagTest.amb.addMeasure(Aggregator.builder().name(name).columnName(column).aggregationKey(key).build());
	}

	@When("Query measure={word} debug={word}")
	public void aUserPublishANewKata(String measure, String debug) {
		AdhocQueryBuilder queryBuilder =
				AdhocQuery.builder().measureRef(ReferencedMeasure.builder().ref(measure).build());

		if (Boolean.valueOf(debug)) {
			queryBuilder.debug(true);
		}

		AdhocQuery query = queryBuilder.build();

		try {
			tabularView = aDagTest.aqe.execute(query, aDagTest.rows);
		} catch (Throwable t) {
			log.trace("A step thrown an exception (which may be expected by the scenario)", t);
			this.t = t;
		}
	}

	@Then("View contains")
	public void heShouldHaveASuccessResponse(DataTable dataTable) {
		List<Map<String, ?>> coordinates = tabularView.keySet().map(m -> m).collect(Collectors.toList());

		dataTable.asMaps().forEach(expectedMap -> {
			Assertions.assertThat(coordinates).contains(expectedMap);
		});
	}

	@Then("Has failed")
	public void hasFailed() {
		Assertions.assertThat(t).isNotNull();
	}
}