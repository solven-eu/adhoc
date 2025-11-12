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
package eu.solven.adhoc.data.row.slice;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.map.ISliceFactory;
import eu.solven.adhoc.query.filter.ISliceFilter;

/**
 * 
 * An {@link IAdhocSlice} which enables switching from different implementations. It is typically useful to switch from
 * a plain {@link Map} implementation into a compressed implementation (e.g. referring a row from a compressed-table
 * structure).
 * 
 * @author Benoit Lacelle
 * @deprecated Not-Ready due to `.hashCode`, `.equals` and `.compare` between IAdhocSlice different implementations.
 */
@Deprecated(since = "Not-Ready")
public class ProxiedSlice implements IAdhocSlice {

	final AtomicReference<IAdhocSlice> refSlice;

	public ProxiedSlice(IAdhocSlice initialSlice) {
		this.refSlice = new AtomicReference<>(initialSlice);
	}

	@Override
	public int compareTo(IAdhocSlice o) {
		return refSlice.get().compareTo(o);
	}

	@Override
	public int hashCode() {
		return refSlice.get().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return refSlice.get().equals(obj);
	}

	@Override
	public IAdhocSlice getGroupBys() {
		return refSlice.get().getGroupBys();
	}

	@Override
	public Set<String> columnsKeySet() {
		return refSlice.get().columnsKeySet();
	}

	@Override
	public @Nullable Object getGroupBy(String column) {
		return refSlice.get().getGroupBy(column);
	}

	@Override
	public void forEachGroupBy(BiConsumer<? super String, ? super Object> action) {
		refSlice.get().forEachGroupBy(action);
	}

	@Override
	public boolean isEmpty() {
		return refSlice.get().isEmpty();
	}

	@Override
	public ISliceFilter asFilter() {
		return refSlice.get().asFilter();
	}

	@Override
	public Optional<Object> optGroupBy(String column) {
		return refSlice.get().optGroupBy(column);
	}

	@Override
	public Map<String, ?> optGroupBy(Set<String> columns) {
		return refSlice.get().optGroupBy(columns);
	}

	@Override
	public IAdhocSlice addColumns(Map<String, ?> masks) {
		return refSlice.get().addColumns(masks);
	}

	@Override
	public ISliceFactory getFactory() {
		return refSlice.get().getFactory();
	}

}