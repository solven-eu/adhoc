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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.example.worldcup.EventAggregation.SimpleAggregationCarrier;
import eu.solven.adhoc.primitive.IValueProvider;

public class TestEventAggregation {

	final EventAggregation aggregation = new EventAggregation();

	@Test
	public void testWrap_singleGoal() {
		SimpleAggregationCarrier result = aggregation.wrap("G13'");

		Assertions.assertThat(result.getT().getTypeToMinuteToCounts()).containsKey("G");
		Assertions.assertThat(result.getT().getTypeToMinuteToCounts().get("G").asMap()).containsEntry(13, 1L);
	}

	@Test
	public void testWrap_multipleEventsInString() {
		SimpleAggregationCarrier result = aggregation.wrap("G11' G18' Y84'");

		PlayersEvents events = result.getT();
		Assertions.assertThat(events.getTypeToMinuteToCounts()).containsKeys("G", "Y");
		Assertions.assertThat(events.getTypeToMinuteToCounts().get("G").asMap())
				.containsEntry(11, 1L)
				.containsEntry(18, 1L);
		Assertions.assertThat(events.getTypeToMinuteToCounts().get("Y").asMap()).containsEntry(84, 1L);
	}

	@Test
	public void testWrap_unsupportedType_throws() {
		Assertions.assertThatThrownBy(() -> aggregation.wrap(42)).isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	public void testAggregate_leftNull_returnsRight() {
		SimpleAggregationCarrier right = aggregation.wrap("G5'");

		Object result = aggregation.aggregate(null, right);

		Assertions.assertThat(result).isSameAs(right);
	}

	@Test
	public void testAggregate_rightNull_returnsLeft() {
		SimpleAggregationCarrier left = aggregation.wrap("G5'");

		Object result = aggregation.aggregate(left, null);

		Assertions.assertThat(result).isSameAs(left);
	}

	@Test
	public void testAggregate_bothCarriers_mergesEvents() {
		SimpleAggregationCarrier left = aggregation.wrap("G5'");
		SimpleAggregationCarrier right = aggregation.wrap("G10'");

		// Both carriers implement IAggregationCarrier extends IValueProvider, so Java resolves to the default
		// IAggregation.aggregate(IValueProvider, IValueProvider) which wraps the result in IValueProvider.setValue(...)
		Object result = aggregation.aggregate(left, right);
		Object unwrapped;
		if (result instanceof IValueProvider vp) {
			unwrapped = IValueProvider.getValue(vp);
		} else {
			unwrapped = result;
		}

		Assertions.assertThat(unwrapped).isInstanceOf(PlayersEvents.class);
		PlayersEvents merged = (PlayersEvents) unwrapped;
		Assertions.assertThat(merged.getTypeToMinuteToCounts().get("G").asMap())
				.containsEntry(5, 1L)
				.containsEntry(10, 1L);
	}

	@Test
	public void testAggregate_leftIsPlayersEvents_mergesWithCarrier() {
		SimpleAggregationCarrier carrier = aggregation.wrap("G5'");
		// Simulate a pre-merged PlayersEvents as left
		PlayersEvents events = carrier.getT();
		SimpleAggregationCarrier right = aggregation.wrap("Y88'");

		Object result = aggregation.aggregate(events, right);

		Assertions.assertThat(result).isInstanceOf(PlayersEvents.class);
		PlayersEvents merged = (PlayersEvents) result;
		Assertions.assertThat(merged.getTypeToMinuteToCounts()).containsKeys("G", "Y");
	}

	@Test
	public void testAggregate_unrecognisedType_throws() {
		Assertions.assertThatThrownBy(() -> aggregation.aggregate("notEvents", "alsoNotEvents"))
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	public void testMaxMinutes_updatedOnWrap() {
		int before = EventAggregation.MAX_MINUTES.get();
		aggregation.wrap("G" + (before + 5) + "'");

		Assertions.assertThat(EventAggregation.MAX_MINUTES.get()).isGreaterThanOrEqualTo(before + 5);
	}
}
