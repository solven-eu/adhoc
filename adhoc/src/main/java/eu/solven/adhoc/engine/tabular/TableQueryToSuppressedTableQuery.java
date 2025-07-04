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
package eu.solven.adhoc.engine.tabular;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.table.TableQueryV2;
import eu.solven.adhoc.table.ITableWrapper;
import lombok.Builder;
import lombok.Value;

/**
 * Helps managing {@link IColumnGenerator} when these columns flows down to the {@link Aggregator}
 * {@link CubeQueryStep}. Indeed, {@link IColumnGenerator} may generate columns which are not relevant for the
 * {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class TableQueryToSuppressedTableQuery {
	// The TableQuery as queried by the DAG: it may still refer generated columns
	TableQueryV2 dagQuery;
	// The TableQuery after having suppressed generated columns
	TableQueryV2 suppressedQuery;

	public Set<String> getSuppressedGroupBy() {
		Set<String> queriedColumns = dagQuery.getGroupBy().getNameToColumn().keySet();
		Set<String> withoutSuppressedColumns = suppressedQuery.getGroupBy().getNameToColumn().keySet();
		Set<String> suppressedView = Sets.difference(queriedColumns, withoutSuppressedColumns);
		return ImmutableSet.copyOf(suppressedView);
	}
}
