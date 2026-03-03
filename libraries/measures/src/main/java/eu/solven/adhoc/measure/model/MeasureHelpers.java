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
package eu.solven.adhoc.measure.model;

import java.util.Objects;

import com.google.common.collect.Lists;

import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.combination.CoalesceCombination;
import lombok.experimental.UtilityClass;

/**
 * Utilities around {@link IMeasure}.
 * 
 * @author Benoit Lacelle
 */
@UtilityClass
public class MeasureHelpers {

	/**
	 * 
	 * @param name
	 * @param underlying
	 * @param moreUnderlyings
	 * @return an IMeasure doing the SUM of underlyings.
	 */
	public static IMeasure sum(String name, String underlying, String... moreUnderlyings) {
		if (moreUnderlyings.length == 0) {
			return alias(name, underlying);
		} else {
			return Combinator.builder().name(name).underlyings(Lists.asList(underlying, moreUnderlyings)).build();
		}
	}

	/**
	 * 
	 * @param alias
	 *            the name of the name, i.e. the name with which one can refer to this alias.
	 * @param aliased
	 *            the name of the aliased measure, i.e. the name of the underlying/actually defined measure
	 * @return
	 */
	public static IMeasure alias(String alias, String aliased) {
		if (Objects.equals(alias, aliased)) {
			return ReferencedMeasure.ref(alias);
		} else {
			return Combinator.builder().name(alias).combinationKey(CoalesceCombination.KEY).underlying(aliased).build();
		}
	}
}
