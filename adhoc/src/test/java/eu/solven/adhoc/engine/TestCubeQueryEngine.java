/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.engine;

import com.google.common.collect.ImmutableMap;
import eu.solven.adhoc.measure.combination.ExpressionCombination;
import eu.solven.adhoc.measure.model.Combinator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.ADagTest;
import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.aggregation.comparable.MaxAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.query.cube.CubeQuery;

public class TestCubeQueryEngine extends ADagTest implements IAdhocTestConstants {
    @Override
    public void feedTable() {
        // No need to feed
    }

    @Test
    public void testConflictingNames() {
        Aggregator k1Max = k1Sum.toBuilder().aggregationKey(MaxAggregation.KEY).build();

        Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(k1Sum, k1Max).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasStackTraceContaining("Can not query multiple measures with same name: {k1=2}");
    }

    @Test
    public void testCycleBetweenQuerySteps() {
        String measureA = "m_A";
        String measureB = "m_B";

        Combinator mAIsMbTimed2 = Combinator.builder()
                .name(measureA)
                .underlying(measureB)
                .combinationKey(ExpressionCombination.KEY)
                .combinationOptions(
                        ImmutableMap.<String, Object>builder().put("expression", "IF(m_B == null, null, m_B * 2)").build())
                .build();

        Combinator mBIsMaDividedBy2 = Combinator.builder()
                .name(measureB)
                .underlying(measureA)
                .combinationKey(ExpressionCombination.KEY)
                .combinationOptions(
                        ImmutableMap.<String, Object>builder().put("expression", "IF(m_A == null, null, m_A / 2)").build())
                .build();

        forest.addMeasure(mAIsMbTimed2);
        forest.addMeasure(mBIsMaDividedBy2);

        Assertions.assertThatThrownBy(() -> cube().execute(CubeQuery.builder().measure(measureA).build()))
                .isInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("in cycle=");
    }

}
