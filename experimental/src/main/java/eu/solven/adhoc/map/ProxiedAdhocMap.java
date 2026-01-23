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
package eu.solven.adhoc.map;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import eu.solven.adhoc.data.row.slice.IAdhocSlice;
import eu.solven.adhoc.map.factory.ISliceFactory;
import eu.solven.adhoc.util.immutable.UnsupportedAsImmutableException;

/**
 * 
 * An {@link IAdhocMap} which enables switching from different implementations. It is typically useful to switch from a
 * plain {@link Map} implementation into a compressed implementation (e.g. referring a row from a compressed-table
 * structure).
 * 
 * @author Benoit Lacelle
 * @deprecated Not-Ready due to `.hashCode`, `.equals` and `.compare` between IAdhocMap different implementations.
 */
@Deprecated(since = "Not-Ready")
public class ProxiedAdhocMap implements IAdhocMap {

	private final AtomicReference<IAdhocMap> refMap;

	public ProxiedAdhocMap(IAdhocMap initialSlice) {
		if (initialSlice == null) {
			throw new IllegalArgumentException("Must not be null");
		}

		this.refMap = new AtomicReference<>(initialSlice);
	}

	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public void updateRef(IAdhocMap slice) {
		if (slice == null) {
			throw new IllegalArgumentException("Must not be null. Was %s".formatted(refMap.get()));
		} else if (slice == this) {
			// Else we would get StackOverflowError
			throw new IllegalArgumentException("Must not be itself. Was %s".formatted(refMap.get()));
		}
	}

	@Override
	public int compareTo(IAdhocMap o) {
		return refMap.get().compareTo(o);
	}

	@Override
	public int hashCode() {
		return refMap.get().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return refMap.get().equals(obj);
	}

	@Override
	public boolean isEmpty() {
		return refMap.get().isEmpty();
	}

	@Override
	public ISliceFactory getFactory() {
		return refMap.get().getFactory();
	}

	@Override
	public int size() {
		return refMap.get().size();
	}

	@Override
	public boolean containsKey(Object key) {
		return refMap.get().containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return refMap.get().containsValue(value);
	}

	@Override
	public Object get(Object key) {
		return refMap.get().get(key);
	}

	@Override
	public Object put(String key, Object value) {
		return refMap.get().put(key, value);
	}

	@Override
	public Object remove(Object key) {
		return refMap.get().remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public void clear() {
		throw new UnsupportedAsImmutableException();
	}

	@Override
	public Set<String> keySet() {
		return refMap.get().keySet();
	}

	@Override
	public Collection<Object> values() {
		return refMap.get().values();
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return refMap.get().entrySet();
	}

	@Override
	public IAdhocSlice asSlice() {
		// BEWARE Should we return a proxy, so that slice is upgraded with the map?
		return refMap.get().asSlice();
	}

	@Override
	public IAdhocMap retainAll(Set<String> columns) {
		return refMap.get().retainAll(columns);
	}

}