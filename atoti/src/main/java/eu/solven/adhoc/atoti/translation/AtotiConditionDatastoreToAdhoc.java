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
package eu.solven.adhoc.atoti.translation;

import java.util.stream.Stream;

import com.qfs.condition.ICondition;
import com.qfs.condition.IConstantCondition;
import com.qfs.condition.IDynamicCondition;
import com.qfs.condition.ImplementationCode;

import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.FilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.RegexMatcher;
import lombok.extern.slf4j.Slf4j;

/**
 * Works with {@link com.qfs.condition.ICondition} but not {@link com.quartetfs.fwk.filtering.ICondition}.
 *
 * Typically used for store filtering.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class AtotiConditionDatastoreToAdhoc {

	public ISliceFilter convertToAdhoc(Object rawCondition) {
		if (rawCondition == null) {
			return ISliceFilter.MATCH_NONE;
		} else if (rawCondition instanceof com.quartetfs.fwk.filtering.ICondition apCondition) {
			log.warn("We should not receive Cube conditions: {}", apCondition);
			return ISliceFilter.MATCH_NONE;
		} else if (rawCondition instanceof ICondition apCondition) {
			if (rawCondition instanceof IConstantCondition constantCondition) {
				ImplementationCode implementationCode = constantCondition.getImplementationCode();

				ICondition[] subConditions = constantCondition.getSubConditions();

				if (implementationCode == ImplementationCode.EQ) {
					return ColumnFilter.isEqualTo(constantCondition.getFields()[0], constantCondition.getOperand());
				} else if (implementationCode == ImplementationCode.LIKE) {
					String regex = String.valueOf(constantCondition.getOperand());
					return ColumnFilter.isMatching(constantCondition.getFields()[0], RegexMatcher.compile(regex));

				} else if (implementationCode == ImplementationCode.AND) {
					return FilterBuilder.and(Stream.of(subConditions).map(this::convertToAdhoc).toList()).optimize();
				} else if (implementationCode == ImplementationCode.OR) {
					return FilterBuilder.or(Stream.of(subConditions).map(this::convertToAdhoc).toList()).optimize();
				} else if (implementationCode == ImplementationCode.TRUE) {
					return ISliceFilter.MATCH_ALL;
				} else if (implementationCode == ImplementationCode.FALSE) {
					return ISliceFilter.MATCH_NONE;
				} else {
					log.warn("This case is not well handled: {}", constantCondition);
					return ISliceFilter.MATCH_NONE;
				}
			} else if (rawCondition instanceof IDynamicCondition dynamicCondition) {
				log.warn("This case is not well handled: {}", dynamicCondition);
				return ISliceFilter.MATCH_NONE;
			} else {
				log.warn("This case is not well handled: {}", apCondition);
				return ISliceFilter.MATCH_NONE;
			}
		} else {
			// Assume we received a raw object
			return ISliceFilter.MATCH_NONE;
		}
	}

}
