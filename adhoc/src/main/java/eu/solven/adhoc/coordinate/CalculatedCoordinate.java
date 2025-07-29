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
package eu.solven.adhoc.coordinate;

import eu.solven.adhoc.query.filter.ISliceFilter;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Default implementation of {@link ICalculatedCoordinate}.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
@Jacksonized
public class CalculatedCoordinate implements ICalculatedCoordinate {
	@NonNull
	protected Object coordinate;

	@NonNull
	@Default
	protected ISliceFilter filter = ISliceFilter.MATCH_ALL;

	/**
	 * Useful grand members representing the grandTotal.
	 * 
	 * @return a {@link ICalculatedCoordinate} matching all available members.
	 */
	public static ICalculatedCoordinate star() {
		return CalculatedCoordinate.builder().coordinate("*").filter(ISliceFilter.MATCH_ALL).build();
	}

}
