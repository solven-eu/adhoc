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
package eu.solven.adhoc.atoti.translation;

import java.util.Arrays;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.qfs.condition.impl.BaseConditions;
import com.quartetfs.fwk.filtering.impl.EqualCondition;
import com.quartetfs.fwk.filtering.impl.FalseCondition;
import com.quartetfs.fwk.filtering.impl.GreaterEqualCondition;
import com.quartetfs.fwk.filtering.impl.InCondition;
import com.quartetfs.fwk.filtering.impl.LowerCondition;
import com.quartetfs.fwk.filtering.impl.OrCondition;
import com.quartetfs.fwk.filtering.impl.TrueCondition;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;

public class TestAtotiConditionCubeToAdhoc {
	AtotiConditionCubeToAdhoc apConditionToAdhoc = new AtotiConditionCubeToAdhoc();

	@Test
	public void testConditionToFilter_Raw() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", "someString"))
				.isEqualTo(ColumnFilter.equalTo("someLevel", "someString"));
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", 123))
				.isEqualTo(ColumnFilter.equalTo("someLevel", 123));
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", Arrays.asList("someString", 123)))
				.isEqualTo(ColumnFilter.matchIn("someLevel", Arrays.asList("someString", 123)));

		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", null))
				.isEqualTo(ColumnFilter.builder().column("someLevel").matchNull().build());
	}

	@Test
	public void testConditionToFilter_Conditions() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", new TrueCondition()))
				.isEqualTo(ColumnFilter.MATCH_ALL);
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", new FalseCondition()))
				.isEqualTo(ColumnFilter.MATCH_NONE);

		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", new EqualCondition("someString")))
				.isEqualTo(ColumnFilter.equalTo("someLevel", "someString"));
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", new InCondition("someString")))
				.isEqualTo(ColumnFilter.matchIn("someLevel", "someString"));
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", new InCondition("someString", 123)))
				.isEqualTo(ColumnFilter.matchIn("someLevel", Set.of("someString", 123)));

		Assertions
				.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel",
						new OrCondition(Arrays.asList(new GreaterEqualCondition(123), new LowerCondition(234)))))
				.isEqualTo(FilterBuilder.or(Arrays.asList(ColumnFilter.builder()
						.column("someLevel")
						.valueMatcher(
								ComparingMatcher.builder().greaterThan(true).matchIfEqual(true).operand(123).build())
						.build(),
						ColumnFilter.builder()
								.column("someLevel")
								.valueMatcher(ComparingMatcher.builder()
										.greaterThan(false)
										.matchIfEqual(false)
										.operand(234)
										.build())
								.build()))
						.optimize());
	}

	@Test
	public void testConditionToFilter_DatastoreConditions() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", BaseConditions.True()))
				.isEqualTo(ColumnFilter.equalTo("someLevel", "TRUE"));
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc("someLevel", BaseConditions.False()))
				.isEqualTo(ColumnFilter.equalTo("someLevel", "FALSE"));
	}
}
