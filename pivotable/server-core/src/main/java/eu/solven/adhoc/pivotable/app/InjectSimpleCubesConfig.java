/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.pivotable.app;

import java.util.Arrays;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import eu.solven.adhoc.app.IPivotableSpringProfiles;
import eu.solven.adhoc.beta.schema.AdhocSchemaForApi;
import eu.solven.adhoc.measure.AdhocMeasureBag;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.sum.SumCombination;
import eu.solven.adhoc.query.AdhocQuery;
import eu.solven.adhoc.table.InMemoryTable;
import lombok.extern.slf4j.Slf4j;

/**
 * Add a simple cube for tests and demo purposes.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class InjectSimpleCubesConfig {

	@Profile(IPivotableSpringProfiles.P_SIMPLE_DATASETS)
	@Bean
	public Void initSimpleCubes(AdhocSchemaForApi schemaForApi) {
		log.info("Registering the {} dataset", IPivotableSpringProfiles.P_SIMPLE_DATASETS);

		InMemoryTable table = InMemoryTable.builder().name("simple").build();
		table.add(Map.of("ccy", "EUR", "delta", 12.34, "gamma", 123.4));
		table.add(Map.of("ccy", "USD", "delta", 23.45, "gamma", 234.5));

		schemaForApi.registerTable(table);

		schemaForApi.registerMeasureBag(AdhocMeasureBag.fromMeasures("simple",
				Arrays.asList(Aggregator.sum("delta"),
						Aggregator.sum("gamma"),
						Combinator.builder()
								.name("delta+gamma")
								.underlying("delta")
								.underlying("gamma")
								.combinationKey(SumCombination.KEY)
								.build())));

		schemaForApi.registerCube("simple", "simple", "simple");

		schemaForApi.registerQuery("delta.EUR", AdhocQuery.builder().measure("delta").andFilter("ccy", "EUR").build());
		schemaForApi.registerQuery("gamma.USD", AdhocQuery.builder().measure("gamma").andFilter("ccy", "USD").build());
		schemaForApi.registerQuery("delta+gamma.grandTotal", AdhocQuery.builder().measure("delta+gamma").build());

		return null;
	}
}
