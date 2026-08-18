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
package eu.solven.adhoc.export.excel;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.IColumnFilter;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import lombok.experimental.UtilityClass;

/**
 * Compiles an {@link ISliceFilter} into an Excel boolean expression suitable for use inside {@code IF(…, x, 0)}.
 * Supported filter shapes — anything else throws with a clear error:
 *
 * <ul>
 * <li>{@link IColumnFilter} with an {@link EqualsMatcher} → {@code <cell>="value"} (or {@code =number}).</li>
 * <li>{@link IColumnFilter} with an {@link InMatcher} → {@code OR(<cell>="v1", <cell>="v2", …)}.</li>
 * <li>{@link IAndFilter} → {@code AND(<op1>, <op2>, …)}.</li>
 * <li>{@link IOrFilter} → {@code OR(<op1>, <op2>, …)}.</li>
 * <li>{@link INotFilter} → {@code NOT(<inner>)}.</li>
 * </ul>
 *
 * <p>
 * Every column referenced by the filter must be available on the row via {@link RowContext#getGroupByCellRef} — callers
 * should pre-check {@link #columnsReferenced(ISliceFilter)} against the groupBy and fall back to inline value when the
 * filter widens beyond the visible columns.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public final class FilterToExcelPredicate {

	/** Compile {@code filter} to an Excel boolean expression using {@code ctx} for column cell references. */
	public static String compile(ISliceFilter filter, RowContext ctx) {
		return walk(filter, ctx);
	}

	/** Returns every column referenced by {@code filter}. Throws if the filter contains an unsupported shape. */
	public static Set<String> columnsReferenced(ISliceFilter filter) {
		Set<String> cols = new LinkedHashSet<>();
		collectColumns(filter, cols);
		return cols;
	}

	private static void collectColumns(ISliceFilter filter, Set<String> cols) {
		if (filter instanceof IColumnFilter cf) {
			cols.add(cf.getColumn());
		} else if (filter instanceof IAndFilter af) {
			af.getOperands().forEach(op -> collectColumns(op, cols));
		} else if (filter instanceof IOrFilter of) {
			of.getOperands().forEach(op -> collectColumns(op, cols));
		} else if (filter instanceof INotFilter nf) {
			collectColumns(nf.getNegated(), cols);
		} else {
			throw new IllegalArgumentException("Unsupported filter shape for Excel translation: " + filter
					+ " (class="
					+ filter.getClass().getSimpleName()
					+ ")");
		}
	}

	private static String walk(ISliceFilter filter, RowContext ctx) {
		if (filter instanceof IColumnFilter cf) {
			return compileColumnFilter(cf, ctx);
		}
		if (filter instanceof IAndFilter af) {
			return "AND(" + joinOps(af.getOperands(), ctx) + ")";
		}
		if (filter instanceof IOrFilter of) {
			return "OR(" + joinOps(of.getOperands(), ctx) + ")";
		}
		if (filter instanceof INotFilter nf) {
			return "NOT(" + walk(nf.getNegated(), ctx) + ")";
		}
		throw new IllegalArgumentException("Unsupported filter shape for Excel translation: " + filter
				+ " (class="
				+ filter.getClass().getSimpleName()
				+ ")");
	}

	private static String compileColumnFilter(IColumnFilter cf, RowContext ctx) {
		String cellRef = ctx.getGroupByCellRef(cf.getColumn());
		IValueMatcher matcher = cf.getValueMatcher();
		if (matcher instanceof EqualsMatcher em) {
			return cellRef + "=" + literal(em.getOperand());
		}
		if (matcher instanceof InMatcher im) {
			// OR(cellRef="v1", cellRef="v2", …). Single-value InMatcher degenerates to a simple equality.
			Collection<?> operands = im.getOperands();
			if (operands.size() == 1) {
				return cellRef + "=" + literal(operands.iterator().next());
			}
			return "OR(" + operands.stream().map(v -> cellRef + "=" + literal(v)).collect(Collectors.joining(","))
					+ ")";
		}
		throw new IllegalArgumentException("Unsupported ColumnFilter matcher for Excel translation: " + matcher
				+ " (only EqualsMatcher and InMatcher supported in v2)");
	}

	private static String joinOps(Collection<? extends ISliceFilter> ops, RowContext ctx) {
		return ops.stream().map(op -> walk(op, ctx)).collect(Collectors.joining(","));
	}

	/** Excel-literal form of a filter operand. Strings are double-quoted (with embedded quotes doubled). */
	private static String literal(Object operand) {
		if (operand == null) {
			return "\"\"";
		}
		if (operand instanceof Number) {
			return operand.toString();
		}
		if (operand instanceof Boolean) {
			if ((Boolean) operand) {
				return "TRUE";
			} else {
				return "FALSE";
			}
		}
		return "\"" + operand.toString().replace("\"", "\"\"") + "\"";
	}
}
