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
package eu.solven.adhoc.table.sql;

import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.Value;

/**
 * Help transforming a {@link org.jooq.Record} into {@link eu.solven.adhoc.data.row.ITabularRecord}
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@ToString(exclude = "allColumns")
public class AggregatedRecordFields {
	@NonNull
	@Singular
	ImmutableList<String> aggregates;
	@NonNull
	@Singular
	ImmutableList<String> columns;
	// Additional columns for the leftover filters
	@NonNull
	@Singular
	ImmutableList<String> leftovers;

	private Supplier<ImmutableList<String>> allColumns =
			Suppliers.memoize(() -> ImmutableList.copyOf(Iterables.concat(getColumns(), getLeftovers())));

	public ImmutableList<String> getAllColumns() {
		return allColumns.get();
	}
}
