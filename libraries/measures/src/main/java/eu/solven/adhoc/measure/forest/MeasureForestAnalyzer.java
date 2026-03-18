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
package eu.solven.adhoc.measure.forest;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.IAndFilter;
import eu.solven.adhoc.filter.INotFilter;
import eu.solven.adhoc.filter.IOrFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.value.EqualsMatcher;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.filter.value.InMatcher;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Shiftor;
import lombok.experimental.UtilityClass;

/**
 * Analyses a {@link IMeasureForest} and surfaces information about the columns it references.
 *
 * <p>
 * Two main analyses are provided:
 * <ol>
 * <li>{@link #columnToSpecialValues(IMeasureForest)} — for every column referenced by the forest, returns the set of
 * "special" values that appear in equality / IN-membership conditions inside {@link ISliceFilter} trees (e.g. inside a
 * {@link Filtrator}).</li>
 * <li>{@link #tagToColumns(IMeasureForest)} — for every tag carried by at least one measure, returns the set of column
 * names referenced by those tagged measures.</li>
 * </ol>
 *
 * @author Benoit Lacelle
 */
@UtilityClass
public class MeasureForestAnalyzer {

	/**
	 * Returns a map from every column name referenced in {@code forest} to the set of "special" values (i.e. equality
	 * or IN-membership operands) that appear in {@link ISliceFilter} trees within the forest.
	 *
	 * <p>
	 * A value is considered "special" when it appears as the operand of an {@link EqualsMatcher} or as one of the
	 * elements of an {@link InMatcher}. Column names with no special values still appear as keys if they are referenced
	 * elsewhere (e.g. in an {@link Aggregator#getColumnName()}).
	 *
	 * @param forest
	 *            the forest to analyse
	 * @return an unmodifiable map; keys are column names, values are the (possibly empty) set of special values
	 */
	public static Map<String, Set<Object>> columnToSpecialValues(IMeasureForest forest) {
		Map<String, Set<Object>> result = new LinkedHashMap<>();

		for (IMeasure m : forest.getNameToMeasure().values()) {
			if (m instanceof Aggregator agg) {
				result.computeIfAbsent(agg.getColumnName(), k -> new LinkedHashSet<>());
			}
			if (m instanceof Filtrator fil) {
				collectFromFilter(fil.getFilter(), result);
			}
			if (m instanceof Shiftor shiftor) {
				shiftor.getEditorOptions().forEach((k, v) -> result.computeIfAbsent(k, col -> new LinkedHashSet<>()));
			}
		}

		// Wrap values sets as unmodifiable
		Map<String, Set<Object>> unmodifiable = new LinkedHashMap<>();
		result.forEach((col, vals) -> unmodifiable.put(col, java.util.Collections.unmodifiableSet(vals)));
		return java.util.Collections.unmodifiableMap(unmodifiable);
	}

	/**
	 * Returns a multimap from every tag present in the forest to the column names referenced by measures carrying that
	 * tag.
	 *
	 * <p>
	 * A column is considered "referenced" by a measure when it appears as an {@link Aggregator#getColumnName()}, as a
	 * column inside a {@link Filtrator}'s filter tree, or as a key in a {@link Shiftor}'s editor-options map.
	 *
	 * @param forest
	 *            the forest to analyse
	 * @return an unmodifiable {@link SetMultimap}; each tag maps to at least one column
	 */
	public static SetMultimap<String, String> tagToColumns(IMeasureForest forest) {
		ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();

		for (IMeasure m : forest.getNameToMeasure().values()) {
			if (m.getTags().isEmpty()) {
				continue;
			}
			Set<String> columns = columnsOf(m);
			for (String tag : m.getTags()) {
				columns.forEach(col -> builder.put(tag, col));
			}
		}

		return builder.build();
	}

	// ── private helpers ───────────────────────────────────────────────────────

	/**
	 * Returns the set of column names directly referenced by a single measure (Aggregator columnName, Filtrator filter
	 * columns, Shiftor option keys).
	 */
	private static Set<String> columnsOf(IMeasure m) {
		Set<String> cols = new LinkedHashSet<>();
		if (m instanceof Aggregator agg) {
			cols.add(agg.getColumnName());
		}
		if (m instanceof Filtrator fil) {
			MeasureForestConcealer.collectFilterColumns(fil.getFilter(), cols);
		}
		if (m instanceof Shiftor shiftor) {
			cols.addAll(shiftor.getEditorOptions().keySet());
		}
		return cols;
	}

	/**
	 * Recursively walks {@code filter} and records every column name → special values into {@code result}.
	 */
	private static void collectFromFilter(ISliceFilter filter, Map<String, Set<Object>> result) {
		if (filter instanceof ColumnFilter cf) {
			Set<Object> values = result.computeIfAbsent(cf.getColumn(), k -> new LinkedHashSet<>());
			collectSpecialValues(cf.getValueMatcher(), values);
		} else if (filter instanceof IAndFilter af) {
			af.getOperands().forEach(f -> collectFromFilter(f, result));
		} else if (filter instanceof IOrFilter of) {
			of.getOperands().forEach(f -> collectFromFilter(f, result));
		} else if (filter instanceof INotFilter nf) {
			collectFromFilter(nf.getNegated(), result);
		}
	}

	/**
	 * Extracts the operand(s) from equality and IN matchers into {@code values}.
	 */
	private static void collectSpecialValues(IValueMatcher matcher, Collection<Object> values) {
		if (matcher instanceof EqualsMatcher eq) {
			values.add(eq.getOperand());
		} else if (matcher instanceof InMatcher in) {
			values.addAll(in.getOperands());
		}
	}

	/**
	 * Returns the column-to-special-values analysis as an {@link ImmutableMap} suitable for logging or serialisation.
	 */
	public static ImmutableMap<String, Set<Object>> columnToSpecialValuesImmutable(IMeasureForest forest) {
		return ImmutableMap.copyOf(columnToSpecialValues(forest));
	}
}
