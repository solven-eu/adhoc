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
package eu.solven.adhoc.security;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.eventbus.EventBus;

import eu.solven.adhoc.cube.CubeWrapper;
import eu.solven.adhoc.data.tabular.MapBasedTabularView;
import eu.solven.adhoc.engine.CubeQueryEngine;
import eu.solven.adhoc.engine.context.IQueryPreparator;
import eu.solven.adhoc.engine.context.StandardQueryPreparator;
import eu.solven.adhoc.eventbus.AdhocEventsFromGuavaEventBusToSfl4j;
import eu.solven.adhoc.measure.UnsafeMeasureForest;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;
import eu.solven.adhoc.table.InMemoryTable;

/**
 * This demonstrate how Spring Security can be used to cnofigure an {@link eu.solven.adhoc.dag.IAdhocImplicitFilter}, to
 * automatically filter available data based on user-rights.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class TestImplicitFilter_SpringSecurity {

	InMemoryTable rows = InMemoryTable.builder().build();
	UnsafeMeasureForest amb = UnsafeMeasureForest.builder().name(this.getClass().getSimpleName()).build();
	CubeWrapper aqw;

	{
		EventBus eventBus = new EventBus();
		eventBus.register(new AdhocEventsFromGuavaEventBusToSfl4j());
		CubeQueryEngine aqe = CubeQueryEngine.builder().eventBus(eventBus::post).build();

		IQueryPreparator queryPreparator =
				StandardQueryPreparator.builder().implicitFilter(new SpringSecurityAdhocImplicitFilter()).build();
		aqw = CubeWrapper.builder()
				.table(rows)
				.engine(aqe)
				.forest(amb)
				.queryPreparator(queryPreparator)
				.eventBus(eventBus::post)
				.build();
	}

	@BeforeEach
	public void testAutomatedFilter() {
		amb.addMeasure(Aggregator.sum("k1"));

		rows.add(Map.of("k1", 123, "ccy", "EUR", "color", "red"));
		rows.add(Map.of("k1", 234, "ccy", "USD", "color", "red"));
		rows.add(Map.of("k1", 345, "ccy", "EUR", "color", "blue"));
		rows.add(Map.of("k1", 456, "ccy", "USD", "color", "blue"));
		rows.add(Map.of("k1", 567, "ccy", "EUR", "color", "green"));
		rows.add(Map.of("k1", 678, "ccy", "USD", "color", "green"));
	}

	@Test
	@WithMockUser(roles = { SpringSecurityAdhocImplicitFilter.ROLE_ADMIN })
	public void testAdmin() {
		MapBasedTabularView view = MapBasedTabularView.load(aqw.execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("k1", 0L + 123 + 234 + 345 + 456 + 567 + 678));
	}

	@Test
	@WithMockUser(roles = { SpringSecurityAdhocImplicitFilter.ROLE_EUR })
	public void testEUR() {
		MapBasedTabularView view = MapBasedTabularView.load(aqw.execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("k1", 0L + 123 + 345 + 567));
	}

	@Test
	@WithMockUser(roles = { "color=red" })
	public void testColorRed() {
		MapBasedTabularView view = MapBasedTabularView.load(aqw.execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("k1", 0L + 123 + 234));
	}

	@Test
	@WithMockUser(roles = { "color=red", "color=blue" })
	public void testColorRedBlue() {
		MapBasedTabularView view = MapBasedTabularView.load(aqw.execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("k1", 0L + 123 + 234 + 345 + 456));
	}

	@Test
	@WithMockUser(roles = { SpringSecurityAdhocImplicitFilter.ROLE_EUR, "color=red", "color=blue" })
	public void testEURColorRedBlue() {
		MapBasedTabularView view = MapBasedTabularView.load(aqw.execute(CubeQuery.builder().measure("k1").build()));

		Assertions.assertThat(view.getCoordinatesToValues())
				.hasSize(1)
				.containsEntry(Map.of(), Map.of("k1", 0L + 123 + 234 + 345 + 456 + 567));
	}
}
