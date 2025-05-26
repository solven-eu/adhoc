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
package eu.solven.adhoc.atoti.convertion;

import java.util.regex.Pattern;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.qfs.condition.impl.BaseConditions;

import eu.solven.adhoc.atoti.convertion.AtotiConditionDatastoreToAdhoc;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestAtotiConditionDatastoreToAdhoc {
	AtotiConditionDatastoreToAdhoc apConditionToAdhoc = new AtotiConditionDatastoreToAdhoc();

	@Test
	public void testAnd() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.True()))
				.isEqualTo(IAdhocFilter.MATCH_ALL);
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.And()))
				.isEqualTo(IAdhocFilter.MATCH_ALL);

		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.And(BaseConditions.Equal("c1", "v1"))))
				.isEqualTo(ColumnFilter.isEqualTo("c1", "v1"));

		Assertions
				.assertThat(apConditionToAdhoc.convertToAdhoc(
						BaseConditions.And(BaseConditions.Equal("c1", "v1"), BaseConditions.Equal("c2", "v2"))))
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2")));
	}

	@Test
	public void testOr() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.False()))
				.isEqualTo(IAdhocFilter.MATCH_NONE);
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.Or()))
				.isEqualTo(IAdhocFilter.MATCH_NONE);

		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.And(BaseConditions.Equal("c1", "v1"))))
				.isEqualTo(ColumnFilter.isEqualTo("c1", "v1"));

		Assertions
				.assertThat(apConditionToAdhoc.convertToAdhoc(
						BaseConditions.And(BaseConditions.Equal("c1", "v1"), BaseConditions.Equal("c2", "v2"))))
				.isEqualTo(AndFilter.and(ColumnFilter.isEqualTo("c1", "v1"), ColumnFilter.isEqualTo("c2", "v2")));
	}

	@Test
	public void testPattern() {
		Assertions.assertThat(apConditionToAdhoc.convertToAdhoc(BaseConditions.Like("c1", "v1")).toString())
				.isEqualTo(ColumnFilter.isMatching("c1", Pattern.compile("v1")).toString());
	}
}
