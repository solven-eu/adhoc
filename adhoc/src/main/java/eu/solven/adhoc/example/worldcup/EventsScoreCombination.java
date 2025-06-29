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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import lombok.extern.slf4j.Slf4j;

/**
 * Some score logic: higher with more goals, lower with more redcards. Redcards are quadratric: 1 is OK, 2 is bad, 3 is
 * disastrous.
 * 
 * @author Benoit Lacelle
 */
@Slf4j
public class EventsScoreCombination implements ICombination {
	@Override
	public Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		Long nbGoals = (Long) underlyingValues.get(0);
		if (nbGoals == null) {
			nbGoals = 0L;
		}

		Long nbRedcards = (Long) underlyingValues.get(1);
		if (nbRedcards == null) {
			nbRedcards = 0L;
		}

		Number nbMatch = (Number) underlyingValues.get(2);
		if (nbMatch == null) {
			throw new IllegalStateException("Can not have a goal or redcard event without a match");
		}

		log.trace("{} {} {} {}", slice.getAdhocSliceAsMap().getCoordinates(), nbGoals, nbRedcards, nbMatch);

		return BigDecimal.valueOf(nbGoals - nbRedcards * nbRedcards)
				.divide(BigDecimal.valueOf(nbMatch.longValue()), RoundingMode.HALF_EVEN)
				.doubleValue();
	}
}
