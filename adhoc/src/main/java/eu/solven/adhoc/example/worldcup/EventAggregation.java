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

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.CharMatcher;
import com.google.common.util.concurrent.AtomicLongMap;

import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.aggregation.carrier.IAggregationCarrier;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.pepper.core.PepperLogHelper;
import lombok.Builder;
import lombok.Value;

/**
 * Accepts events as {@link String} like `G10'`, and turns them into a PlayerEvents.
 * 
 * @author Benoit Lacelle
 */
public class EventAggregation implements IAggregation, IAggregationCarrier.IHasCarriers {
	/**
	 * Store the maximum minute encountered through data, up to now.
	 */
	static final AtomicInteger MAX_MINUTES = new AtomicInteger(0);

	/**
	 * Used to convert the raw {@link String} into an {@link IAggregationCarrier} holding the {@link PlayersEvents}.
	 * 
	 * @author Benoit Lacelle
	 */
	@Builder
	@Value
	public static class SimpleAggregationCarrier implements IAggregationCarrier {
		PlayersEvents t;

		@Override
		public void acceptValueReceiver(IValueReceiver valueReceiver) {
			valueReceiver.onObject(t);
		}
	}

	@Override
	public SimpleAggregationCarrier wrap(Object v) {
		if (v instanceof String s) {
			Map<String, AtomicLongMap<Integer>> typeToMinuteToCount = new TreeMap<>();

			splitAndCount(s, typeToMinuteToCount);

			return SimpleAggregationCarrier.builder()
					.t(PlayersEvents.builder().typeToMinuteToCounts(typeToMinuteToCount).build())
					.build();
		} else if (v instanceof java.sql.Array array) {
			try {
				Object[] rawArray = (Object[]) array.getArray();

				Map<String, AtomicLongMap<Integer>> typeToMinuteToCount = new TreeMap<>();

				for (Object o : rawArray) {
					if (o == null) {
						continue;
					}

					splitAndCount(o.toString(), typeToMinuteToCount);

				}

				return SimpleAggregationCarrier.builder()
						.t(PlayersEvents.builder().typeToMinuteToCounts(typeToMinuteToCount).build())
						.build();
			} catch (SQLException e) {
				throw new IllegalStateException("Issue with v=%s".formatted(PepperLogHelper.getObjectAndClass(v)), e);
			}
		} else {
			throw new UnsupportedOperationException(
					"Not managed: v=%s".formatted(PepperLogHelper.getObjectAndClass(v)));
		}
	}

	protected void splitAndCount(String s, Map<String, AtomicLongMap<Integer>> typeToMinuteToCount) {
		// e.g. `G11' G18' Y84' O88'`
		for (String eventAsString : s.split(" ")) {
			int lastCodeCharIndex = CharMatcher.inRange('A', 'Z').lastIndexIn(eventAsString);
			int eventLength = lastCodeCharIndex + 1;

			String eventType = eventAsString.substring(0, eventLength);

			// Skip the trailing `'`
			String minute = eventAsString.substring(eventLength, eventAsString.length() - 1);

			int minuteAsInt = Integer.parseInt(minute);

			if (minuteAsInt > MAX_MINUTES.get()) {
				MAX_MINUTES.set(minuteAsInt);
			}

			typeToMinuteToCount.computeIfAbsent(eventType, k -> AtomicLongMap.create()).incrementAndGet(minuteAsInt);
		}
	}

	@Override
	public Object aggregate(Object left, Object right) {
		if (left == null) {
			return right;
		} else if (right == null) {
			return left;
		} else {
			Optional<PlayersEvents> leftAsEvent = asEvents(left);
			Optional<PlayersEvents> rightAsEvent = asEvents(right);

			if (leftAsEvent.isPresent() && rightAsEvent.isPresent()) {
				return PlayersEvents.merge(leftAsEvent.get(), rightAsEvent.get());
			} else {
				throw new UnsupportedOperationException("left=%s right=%s".formatted(left, right));
			}
		}
	}

	protected Optional<PlayersEvents> asEvents(Object o) {
		if (o instanceof SimpleAggregationCarrier carrier) {
			return Optional.of(carrier.getT());
		} else if (o instanceof PlayersEvents events) {
			return Optional.of(events);
		} else {
			return Optional.empty();
		}
	}

}
