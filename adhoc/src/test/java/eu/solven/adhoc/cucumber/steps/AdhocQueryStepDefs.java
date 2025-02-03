/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.cucumber.steps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.database.IAdhocTableWrapper;
import eu.solven.adhoc.database.InMemoryTable;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.query.AdhocQuery.AdhocQueryBuilder;
import eu.solven.adhoc.transformers.Aggregator;
import eu.solven.adhoc.transformers.ReferencedMeasure;
import eu.solven.adhoc.view.ITabularView;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.slf4j.Slf4j;

/**
 * Basic step-definitions where the {@link IAdhocTableWrapper} is a {@link InMemoryTable}.
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
			tabularView = aDagTest.aqe.execute(query, aDagTest.amb, aDagTest.rows);
		} catch (Throwable t) {
			log.trace("A step thrown an exception (which may be expected by the scenario)", t);
			this.t = t;
		}
	}

	@Then("View contains")
	public void heShouldHaveASuccessResponse(DataTable dataTable) {
		if (tabularView == null) {
			if (t == null) {
				throw new IllegalStateException("Have you executed any query?");
			} else {
				throw new IllegalStateException("The query failed", t);
			}
		}

		List<Map<String, ?>> coordinates =
				tabularView.slices().map(m -> m.getCoordinates()).collect(Collectors.toList());

		dataTable.asMaps().forEach(expectedMap -> {
			Assertions.assertThat(coordinates).contains(expectedMap);
		});
	}

	@Then("Has failed")
	public void hasFailed() {
		Assertions.assertThat(t).isNotNull();
	}
}