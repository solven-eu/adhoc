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
package eu.solven.adhoc.measure.filter.rsql;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.rsql.RsqlToAdhocFilter;
import eu.solven.adhoc.query.filter.IAdhocFilter;

public class TestRsqlToAdhocFilter {
	RsqlToAdhocFilter rsqlToAdhocFilter = new RsqlToAdhocFilter();

	@Test
	public void testAnd() {
		IAdhocFilter filter = rsqlToAdhocFilter.rsql("name==\"Kill Bill\";year=gt=2003");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"name=Kill Bill&year matches `ComparingMatcher(operand=2003, greaterThan=true, matchIfEqual=false, matchIfNull=false)`");
	}

	@Test
	public void testAnd_v2() {
		IAdhocFilter filter = rsqlToAdhocFilter.rsql("name==\"Kill Bill\" and year>2003");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"name=Kill Bill&year matches `ComparingMatcher(operand=2003, greaterThan=true, matchIfEqual=false, matchIfNull=false)`");
	}

	@Test
	public void testOr() {
		IAdhocFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action);(director=='Christopher Nolan',actor==*Bale);year=ge=2000");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"genres matches `InMatcher{size=2, #0=sci-fi, #1=action}`&director=Christopher Nolan|actor=*Bale&year matches `ComparingMatcher(operand=2000, greaterThan=true, matchIfEqual=true, matchIfNull=false)`");
	}

	@Test
	public void testOr_v2() {
		IAdhocFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action) and (director=='Christopher Nolan' or actor==*Bale) and year>=2000");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"genres matches `InMatcher{size=2, #0=sci-fi, #1=action}`&director=Christopher Nolan|actor=*Bale&year matches `ComparingMatcher(operand=2000, greaterThan=true, matchIfEqual=true, matchIfNull=false)`");
	}

	@Test
	public void testComparison() {
		IAdhocFilter filter = rsqlToAdhocFilter.rsql("director.lastName==Nolan;year=ge=2000;year=lt=2010");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"director.lastName=Nolan&year matches `ComparingMatcher(operand=2000, greaterThan=true, matchIfEqual=true, matchIfNull=false)`&year matches `ComparingMatcher(operand=2010, greaterThan=false, matchIfEqual=false, matchIfNull=false)`");
	}

	@Test
	public void testComparison_v2() {
		IAdhocFilter filter = rsqlToAdhocFilter.rsql("director.lastName==Nolan and year>=2000 and year<2010");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"director.lastName=Nolan&year matches `ComparingMatcher(operand=2000, greaterThan=true, matchIfEqual=true, matchIfNull=false)`&year matches `ComparingMatcher(operand=2010, greaterThan=false, matchIfEqual=false, matchIfNull=false)`");
	}

	@Test
	public void testComparison_v3() {
		IAdhocFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action);genres=out=(romance,animated,horror),director==Que*Tarantino");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"genres matches `InMatcher{size=2, #0=sci-fi, #1=action}`&NotFilter(negated=genres matches `InMatcher{size=3, #0=romance, #1=animated, #2=horror}`)|director=Que*Tarantino");
	}

	@Test
	public void testComparison_v4() {
		IAdhocFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action) and genres=out=(romance,animated,horror) or director==Que*Tarantino");
		Assertions.assertThat(filter.toString())
				.isEqualTo(
						"genres matches `InMatcher{size=2, #0=sci-fi, #1=action}`&NotFilter(negated=genres matches `InMatcher{size=3, #0=romance, #1=animated, #2=horror}`)|director=Que*Tarantino");
	}
}
