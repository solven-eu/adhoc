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
package eu.solven.adhoc.filter.rsql;

import java.util.List;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.ComparisonOperator;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLOperators;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;
import eu.solven.adhoc.query.filter.AndFilter;
import eu.solven.adhoc.query.filter.ColumnFilter;
import eu.solven.adhoc.query.filter.ColumnFilter.ColumnFilterBuilder;
import eu.solven.adhoc.query.filter.ISliceFilter;
import eu.solven.adhoc.query.filter.NotFilter;
import eu.solven.adhoc.query.filter.OrFilter;
import eu.solven.adhoc.query.filter.value.ComparingMatcher;

/**
 * {@link RSQLVisitor} generating {@link ISliceFilter}.
 * 
 * @author Benoit Lacelle
 */
public class AdhocFilterRsqlVisitor implements RSQLVisitor<ISliceFilter, String> {
	@Override
	public ISliceFilter visit(AndNode node, String param) {
		List<ISliceFilter> operands = node.getChildren().stream().map(operand -> operand.accept(this, param)).toList();

		return AndFilter.and(operands);
	}

	@Override
	public ISliceFilter visit(OrNode node, String param) {
		List<ISliceFilter> operands = node.getChildren().stream().map(operand -> operand.accept(this, param)).toList();

		return OrFilter.or(operands);
	}

	@Override
	public ISliceFilter visit(ComparisonNode node, String param) {
		ComparisonOperator op = node.getOperator();
		String column = node.getSelector();

		// RSQL guarantees that node has at least one argument
		List<String> arguments = node.getArguments();
		Object argument = arguments.get(0);

		if (RSQLOperators.EQUAL.equals(op)) {
			return ColumnFilter.isEqualTo(column, argument);
		}
		if (RSQLOperators.NOT_EQUAL.equals(op)) {
			return NotFilter.not(ColumnFilter.isEqualTo(column, argument));
		}

		if (RSQLOperators.IN.equals(op)) {
			return ColumnFilter.builder().column(column).matchIn(arguments).build();
		}
		if (RSQLOperators.NOT_IN.equals(op)) {
			return NotFilter.not(ColumnFilter.builder().column(column).matchIn(arguments).build());
		}

		// if (!Comparable.class.isAssignableFrom(type)) {
		// throw new IllegalArgumentException(
		// String.format("Operator %s can be used only for Comparables", op));
		// }
		// Comparable comparable = (Comparable) conversionService.convert(argument, type);

		ColumnFilterBuilder columnFilterBuilder = ColumnFilter.builder().column(column);
		ComparingMatcher valueMatcher;

		if (RSQLOperators.GREATER_THAN.equals(op)) {
			valueMatcher = ComparingMatcher.builder().greaterThan(true).matchIfEqual(false).operand(argument).build();
		} else if (RSQLOperators.GREATER_THAN_OR_EQUAL.equals(op)) {
			valueMatcher = ComparingMatcher.builder().greaterThan(true).matchIfEqual(true).operand(argument).build();
		} else if (RSQLOperators.LESS_THAN.equals(op)) {
			valueMatcher = ComparingMatcher.builder().greaterThan(false).matchIfEqual(false).operand(argument).build();
		} else if (RSQLOperators.LESS_THAN_OR_EQUAL.equals(op)) {
			valueMatcher = ComparingMatcher.builder().greaterThan(false).matchIfEqual(true).operand(argument).build();
		} else {
			throw new IllegalArgumentException("Unknown operator: " + op);

		}

		return columnFilterBuilder.valueMatcher(valueMatcher).build();
	}
}