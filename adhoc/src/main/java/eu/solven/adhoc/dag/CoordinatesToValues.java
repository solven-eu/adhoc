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
package eu.solven.adhoc.dag;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.solven.adhoc.slice.AdhocSliceAsMap;
import eu.solven.adhoc.slice.IAdhocSlice;
import eu.solven.adhoc.storage.IRowConverter;
import eu.solven.adhoc.storage.IRowScanner;
import eu.solven.adhoc.storage.IValueConsumer;
import eu.solven.adhoc.storage.MultiTypeStorage;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * This is a simple way to storage the value for a {@link java.util.Set} of {@link IAdhocSlice}.
 */
@Value
@Builder
public class CoordinatesToValues implements ISliceToValues {
	@NonNull
	@Default
	MultiTypeStorage<AdhocSliceAsMap> storage = MultiTypeStorage.<AdhocSliceAsMap>builder().build();

	public static CoordinatesToValues empty() {
		return CoordinatesToValues.builder().build();
	}

	@Override
	public void onValue(IAdhocSlice slice, IValueConsumer consumer) {
		storage.onValue(slice.getAdhocSliceAsMap(), consumer);
	}

	@Override
	public Set<AdhocSliceAsMap> slicesSet() {
		return getStorage().keySetStream().collect(Collectors.toSet());
	}

	@Override
	public void put(AdhocSliceAsMap coordinate, Object value) {
		storage.put(coordinate, value);
	}

	@Override
	public void forEachSlice(IRowScanner<AdhocSliceAsMap> rowScanner) {
		storage.scan(rowScanner);
	}

	@Override
	public <U> Stream<U> stream(IRowConverter<AdhocSliceAsMap, U> rowScanner) {
		return storage.stream(rowScanner);
	}

	@Override
	public long size() {
		return storage.size();
	}
}
