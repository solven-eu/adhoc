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
package eu.solven.adhoc.data.tabular;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.experimental.SuperBuilder;

/**
 * Common behavior to List/Columnar {@link ATabularView}.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public abstract class AListBasedTabularView extends ATabularView {

	public void checkIsDistinct() {
		Set<Map<String, ?>> slices = new HashSet<>();

		slices().forEach(slice -> {
			Map<String, ?> coordinate = slice.getCoordinates();
			if (!slices.add(coordinate)) {
				throw new IllegalStateException("Multiple slices with c=%s. It is illegal in %s".formatted(coordinate,
						this.getClass().getName()));
			}
		});
	}
}
