/**
 * The MIT License
 * Copyright (c) 2026 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.table;

import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.engine.IHasExecutorAndSliceFactory;
import eu.solven.adhoc.engine.context.IIsCancellable;
import eu.solven.adhoc.measure.IHasMeasures;
import eu.solven.adhoc.measure.forest.IMeasureResolver;
import eu.solven.adhoc.options.IHasOptionsAndExecutorService;
import eu.solven.adhoc.query.AdhocQueryId;

/**
 * The slice of a query context that an {@link ITableWrapper} actually consumes — executor service, options, slice
 * factory, query id, the wrapped table, cancellation hooks. Deliberately decoupled from cube/engine concerns (forest,
 * measures, columns manager, …) so the table-side stack can move into a lower-level module without dragging the cube
 * layer along.
 *
 * <p>
 * The cube/engine layer's {@code QueryPod} implements this interface, so existing call-sites that pass a
 * {@code QueryPod} keep working. The reverse is not true: a thin {@code ITableQueryPod} is enough to drive any
 * {@link ITableWrapper#streamSlices} call, but it does not provide the cube context.
 *
 * @author Benoit Lacelle
 */
public interface ITableQueryPod extends IHasOptionsAndExecutorService, IHasExecutorAndSliceFactory, IIsCancellable,
		IMeasureResolver, IHasMeasures {

	/**
	 * @return the unique id of the cube/table query the pod is serving — used by table impls for tracing and cache
	 *         keys.
	 */
	AdhocQueryId getQueryId();

	/**
	 * @return the {@link ITableWrapper} the pod is bound to. Concrete table impls assert {@code this == getTable()} to
	 *         catch context-mismatch bugs early.
	 */
	ITableWrapper getTable();

	/**
	 * Returns a sibling pod with everything identical except the wrapped table replaced by {@code table}. Used by
	 * decorating wrappers (e.g. {@code CachingTableWrapper}) to delegate {@link ITableWrapper#streamSlices} onto an
	 * inner table while preserving the rest of the context.
	 *
	 * @param table
	 *            the table to bind the new pod to.
	 * @return a fresh {@link ITableQueryPod} bound to {@code table}.
	 */
	ITableQueryPod withTable(ITableWrapper table);

	IColumnsManager getColumnsManager();

	/**
	 * Returns a thin standalone {@link ITableQueryPod} bound to {@code table}. Suitable for metadata calls and tests
	 * that drive an {@link ITableWrapper#streamSlices} without a full cube/engine context.
	 */
	static ITableQueryPod forTable(ITableWrapper table) {
		return StandaloneTableQueryPod.builder().table(table).build();
	}

}
