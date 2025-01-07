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

import java.util.Map;
import java.util.Set;

import eu.solven.adhoc.RowScanner;
import eu.solven.adhoc.slice.IAdhocSlice;
import eu.solven.adhoc.storage.MultiTypeStorage;
import eu.solven.adhoc.storage.ValueConsumer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;

/**
 * This is a simple way to storage the value for a {@link java.util.Set} of {@link IAdhocSlice}.
 */
@Value
@Builder
public class CoordinatesToValues implements ICoordinatesToValues {
	@NonNull
	@Default
	MultiTypeStorage<Map<String, ?>> storage = MultiTypeStorage.<Map<String, ?>>builder().build();

	public static CoordinatesToValues empty() {
		return CoordinatesToValues.builder().build();
	}

	@Override
	public void onValue(IAdhocSlice slice, ValueConsumer consumer) {
		storage.onValue(slice.getCoordinates(), consumer);
	}

	@Override
	public Set<Map<String, ?>> keySet() {
		return getStorage().keySet();
	}

	public void put(Map<String, ?> coordinate, Object value) {
		storage.put(coordinate, value);
	}

	@Override
	public void scan(RowScanner<Map<String, ?>> rowScanner) {
		storage.scan(rowScanner);
	}
}
