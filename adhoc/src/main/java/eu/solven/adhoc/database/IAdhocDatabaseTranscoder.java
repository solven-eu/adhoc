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
package eu.solven.adhoc.database;

/**
 * Holds the logic mapping from the columns names in {@link eu.solven.adhoc.api.v1.IAdhocQuery} and columnNames in
 * {@link IAdhocDatabaseWrapper}.
 * <p>
 * This enables re-using a {@link eu.solven.adhoc.dag.AdhocMeasureBag} for different {@link IAdhocDatabaseWrapper}.
 */
public interface IAdhocDatabaseTranscoder {
	/**
	 *
	 * @param queried
	 *            a column name typically used by an {@link eu.solven.adhoc.api.v1.IAdhocQuery}.
	 * @return the equivalent underlying column name, typically used by the database.
	 */
	String underlying(String queried);

	/**
	 *
	 * @param underlying
	 *            a column name typically used by the database.
	 * @return the equivalent queried column name, typically used by an {@link eu.solven.adhoc.api.v1.IAdhocQuery}.
	 */
	String queried(String underlying);
}
