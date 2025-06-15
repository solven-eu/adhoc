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
package eu.solven.adhoc.example.worldcup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import eu.solven.adhoc.beta.schema.CoordinatesSample;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.decomposition.DecompositionHelpers;
import eu.solven.adhoc.measure.decomposition.IDecomposition;
import eu.solven.adhoc.measure.decomposition.IDecompositionEntry;
import eu.solven.adhoc.query.cube.IWhereGroupByQuery;
import eu.solven.adhoc.query.filter.value.IValueMatcher;

/**
 * Decomposes the {@link PlayersEvents} along `minute` and `event_code` columns.
 * 
 * @author Benoit Lacelle
 */
public class DispatchedEvents implements IDecomposition {

	@Override
	public CoordinatesSample getCoordinates(String column, IValueMatcher valueMatcher, int limit) {
		return switch (column) {
		case "minute": {
			int nbMinutes = EventAggregation.MAX_MINUTES.get();
			yield CoordinatesSample.builder()
					.coordinates(IntStream.range(0, nbMinutes).mapToObj(i -> i).toList())
					.estimatedCardinality(nbMinutes)
					.build();
		}
		case "event_code": {
			yield CoordinatesSample.builder().coordinates(Arrays.asList("G", "I")).estimatedCardinality(2).build();
		}
		default:
			yield CoordinatesSample.empty();
		};
	}

	@Override
	public Map<String, Class<?>> getColumnTypes() {
		return Map.of("minute", int.class, "event_code", String.class);
	}

	@Override
	public List<IDecompositionEntry> decompose(ISliceWithStep slice, Object value) {
		if (!(value instanceof PlayersEvents playerEvents)) {
			// May be some sort of error value
			return List.of(IDecompositionEntry.of(Map.of(), value));
		}

		List<IDecompositionEntry> decompositions = new ArrayList<>();

		playerEvents.getTypeToMinuteToCount().forEach((eventCode, minuteToCount) -> {
			minuteToCount.asMap().forEach((minute, count) -> {
				decompositions.add(IDecompositionEntry.of(Map.of("minute", minute, "event_code", eventCode),
						vr -> vr.onLong(count)));
			});
		});

		return decompositions;
	}

	@Override
	public List<IWhereGroupByQuery> getUnderlyingSteps(CubeQueryStep step) {
		return List.of(DecompositionHelpers.suppressColumn(step, getColumnTypes().keySet()));
	}

}
