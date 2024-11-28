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
package eu.solven.adhoc.transformers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import eu.solven.adhoc.aggregations.ICombination;
import eu.solven.adhoc.aggregations.IOperatorsFactory;
import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IColumnFilter;
import eu.solven.adhoc.coordinate.MapComparators;
import eu.solven.adhoc.dag.AdhocQueryStep;
import eu.solven.adhoc.dag.CoordinatesToValues;
import eu.solven.adhoc.storage.AsObjectValueConsumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnatorQueryStep extends CombinatorQueryStep {
	final Columnator columnator;

	public ColumnatorQueryStep(Columnator columnator, IOperatorsFactory transformationFactory, AdhocQueryStep step) {
		super(columnator, transformationFactory, step);

		this.columnator = columnator;
	}

	@Override
	public List<AdhocQueryStep> getUnderlyingSteps() {
		Optional<String> optMissingColumn =
				columnator.getRequiredColumns().stream().filter(c -> this.isMissing(c)).findAny();

		if (optMissingColumn.isPresent()) {
			return Collections.emptyList();
		}

		return super.getUnderlyingSteps();
	}

	private boolean isMissing(String c) {
		return !step.getGroupBy().getGroupedByColumns().contains(c) && !(isMonoSelected(c, step.getFilter()));
	}

	private boolean isMonoSelected(String column, IAdhocFilter filter) {
		if (filter.isColumnMatcher() && filter instanceof IColumnFilter columnFilter
				&& columnFilter.getColumn().equals(column)) {
			return true;
		}

		return false;
	}

	@Override
	public CoordinatesToValues produceOutputColumn(List<CoordinatesToValues> underlyings) {
		if (underlyings.size() != getUnderlyingNames().size()) {
			throw new IllegalArgumentException("underlyingNames.size() != underlyings.size()");
		} else if (underlyings.isEmpty()) {
			return CoordinatesToValues.empty();
		}

		CoordinatesToValues output = CoordinatesToValues.builder().build();

		ICombination tranformation = transformationFactory.makeTransformation(combinator);

		for (Map<String, ?> coordinate : BucketorQueryStep.keySet(combinator.isDebug(), underlyings)) {
			List<Object> underlyingVs = underlyings.stream().map(storage -> {
				AtomicReference<Object> refV = new AtomicReference<>();
				AsObjectValueConsumer consumer = AsObjectValueConsumer.consumer(o -> {
					refV.set(o);
				});

				storage.onValue(coordinate, consumer);

				return refV.get();
			}).collect(Collectors.toList());

			Object value = tranformation.combine(underlyingVs);
			output.put(coordinate, value);
		}

		return output;
	}

	public static Iterable<? extends Map<String, ?>> keySet(boolean debug, List<CoordinatesToValues> underlyings) {
		Set<Map<String, ?>> keySet;
		if (debug) {
			// Enforce an iteration order for debugging-purposes
			keySet = new TreeSet<>(MapComparators.mapComparator());
		} else {
			keySet = new HashSet<>();
		}

		for (CoordinatesToValues underlying : underlyings) {
			keySet.addAll(underlying.getStorage().keySet());
		}

		return keySet;
	}
}
