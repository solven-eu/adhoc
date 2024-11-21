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
package eu.solven.adhoc.aggregations;

public interface IStandardOperators {

	/**
	 * Sum inputs with standard addition. If a NaN is encountered, the aggregate is NaN.
	 */
	String SUM = "SUM";
	/**
	 * Sum inputs with standard addition. If a NaN is encountered, it is excluded from the aggregation.
	 */
	String SAFE_SUM = "SAFE_SUM";
	/**
	 * Count the number of considered input records (similarly to SQL)
	 */
	String COUNT = "COUNT";
	/**
	 * Count the number of cells considered in the query. It helps understanding the granularity of the considered data,
	 * or the presence/lack of intermediate cubes.
	 */
	String CELLCOUNT = "CELLCOUNT";

	@Deprecated(since = "avg should be computed as the ratio of SUM / COUNT")
	String AVG = "AVG";

}