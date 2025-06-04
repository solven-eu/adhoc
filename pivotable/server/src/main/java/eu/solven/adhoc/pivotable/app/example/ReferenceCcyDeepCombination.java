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
package eu.solven.adhoc.pivotable.app.example;

import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.measure.custom_marker.ACustomMarkerCombination;
import eu.solven.adhoc.measure.model.IMeasure;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link ICombination} returning the reference currency parameter. it can be used as underlying of other
 * {@link IMeasure}, or just used for the User to check this value.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class ReferenceCcyDeepCombination extends ACustomMarkerCombination {
	public static final String CCY_DEFAULT = "EUR";
	public static final String PATH_DEEP_CCY = "$.deep.ccy";

	@Override
	protected Object getDefault() {
		return CCY_DEFAULT;
	}

	@Override
	protected String getJoinedMapPath() {
		return PATH_DEEP_CCY;
	}
}
