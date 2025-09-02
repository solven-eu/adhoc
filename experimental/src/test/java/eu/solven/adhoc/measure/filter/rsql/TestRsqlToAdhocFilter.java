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
import eu.solven.adhoc.query.filter.ISliceFilter;

public class TestRsqlToAdhocFilter {
	RsqlToAdhocFilter rsqlToAdhocFilter = new RsqlToAdhocFilter();

	@Test
	public void testAnd() {
		ISliceFilter filter = rsqlToAdhocFilter.rsql("name==\"Kill Bill\";year=gt=2003");
		Assertions.assertThat(filter.toString()).isEqualTo("name==Kill Bill&year>2003");
	}

	@Test
	public void testAnd_v2() {
		ISliceFilter filter = rsqlToAdhocFilter.rsql("name==\"Kill Bill\" and year>2003");
		Assertions.assertThat(filter.toString()).isEqualTo("name==Kill Bill&year>2003");
	}

	@Test
	public void testOr() {
		ISliceFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action);(director=='Christopher Nolan',actor==*Bale);year=ge=2000");
		Assertions.assertThat(filter.toString())
				.isEqualTo("genres=in=(sci-fi,action)&(director==Christopher Nolan|actor==*Bale)&year>=2000");
	}

	@Test
	public void testOr_v2() {
		ISliceFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action) and (director=='Christopher Nolan' or actor==*Bale) and year>=2000");
		Assertions.assertThat(filter.toString())
				.isEqualTo("genres=in=(sci-fi,action)&(director==Christopher Nolan|actor==*Bale)&year>=2000");
	}

	@Test
	public void testComparison() {
		ISliceFilter filter = rsqlToAdhocFilter.rsql("director.lastName==Nolan;year=ge=2000;year=lt=2010");
		Assertions.assertThat(filter.toString()).isEqualTo("director.lastName==Nolan&year>=2000&year<2010");
	}

	@Test
	public void testComparison_v2() {
		ISliceFilter filter = rsqlToAdhocFilter.rsql("director.lastName==Nolan and year>=2000 and year<2010");
		Assertions.assertThat(filter.toString()).isEqualTo("director.lastName==Nolan&year>=2000&year<2010");
	}

	@Test
	public void testComparison_v3() {
		ISliceFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action);genres2=out=(romance,animated,horror),director==Que*Tarantino");
		Assertions.assertThat(filter.toString())
				// TODO Ordering issue in FilterOptimizerHelpers.packColumnFilters
				// .isEqualTo("genres=in=(sci-fi,action)&genres2=out=(romance,animated,horror)|director==Que*Tarantino")
				.isEqualTo("director==Que*Tarantino|genres=in=(sci-fi,action)&genres2=out=(romance,animated,horror)");
	}

	@Test
	public void testComparison_v4() {
		ISliceFilter filter = rsqlToAdhocFilter
				.rsql("genres=in=(sci-fi,action) and genres2=out=(romance,animated,horror) or director==Que*Tarantino");
		Assertions.assertThat(filter.toString())
				// TODO Ordering issue in FilterOptimizerHelpers.packColumnFilters
				// .isEqualTo("genres=in=(sci-fi,action)&genres2=out=(romance,animated,horror)|director==Que*Tarantino")
				.isEqualTo("director==Que*Tarantino|genres=in=(sci-fi,action)&genres2=out=(romance,animated,horror)");
	}
}
