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
package eu.solven.adhoc.dag.tabular;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import eu.solven.adhoc.dag.step.AdhocQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.transformator.column_generator.IColumnGenerator;
import eu.solven.adhoc.query.table.TableQuery;
import lombok.Builder;
import lombok.Value;

/**
 * Helps managing {@link IColumnGenerator} when these columns flows down to the {@link Aggregator}
 * {@link AdhocQueryStep}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class TableQueryToActualTableQuery {
	// The TableQuery as queried by the DAG: it may still refer generated columns
	TableQuery dagTableQuery;
	// The TableQuery after having suppressed generated columns
	TableQuery suppressedTableQuery;

	public Set<String> getSuppressedGroupBy() {
		Set<String> queriedColumns = dagTableQuery.getGroupBy().getNameToColumn().keySet();
		Set<String> withoutSuppressedColumns = suppressedTableQuery.getGroupBy().getNameToColumn().keySet();
		SetView<String> suppressedView = Sets.difference(queriedColumns, withoutSuppressedColumns);
		return ImmutableSet.copyOf(suppressedView);
	}
}
