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
package eu.solven.adhoc.measure;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.IAdhocTestConstants;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.forest.IMeasureForestVisitor;
import eu.solven.adhoc.measure.forest.MeasureForest;
import eu.solven.adhoc.measure.forest.MeasureForestHelpers;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.sum.SumCombination;

public class TestMeasureForest implements IAdhocTestConstants {
	@Test
	public void testAddTag() {
		IMeasureForest baseForest =
				MeasureForest.builder().name("base").measure(Aggregator.countAsterisk()).measure(filterK1onA1).build();

		IMeasureForest updatedForest = baseForest.acceptVisitor(new IMeasureForestVisitor() {
			@Override
			public Set<IMeasure> mapMeasure(IMeasure measure) {
				if (measure.getName().equals(filterK1onA1.getName())) {
					return Set.of(((Filtrator) measure).toBuilder().tag("someTag").build());
				} else {
					return Set.of(measure);
				}
			}
		});

		// Check the base forest has not been mutated
		Assertions.assertThat(baseForest.getMeasures()).hasSize(2).anySatisfy(m -> {
			Assertions.assertThat(m.getName()).isEqualTo(filterK1onA1.getName());
			Assertions.assertThat(m.getTags()).isEmpty();
		});

		// Check the updated forest has been mutated
		Assertions.assertThat(updatedForest.getMeasures()).hasSize(2).anySatisfy(m -> {
			Assertions.assertThat(m.getName()).isEqualTo(filterK1onA1.getName());
			Assertions.assertThat(m.getTags()).containsExactly("someTag");
		});
	}

	// ---- subForestOf ----

	/**
	 * @return a forest containing 4 distinct measures: {@code k1}, {@code k2}, {@code filterK1onA1} (depends on k1),
	 *         and {@code k1PlusK2} (combinator depending on k1 + k2).
	 */
	private IMeasureForest baseForest() {
		Combinator k1PlusK2 = Combinator.builder()
				.name("k1PlusK2")
				.underlyings(List.of("k1", "k2"))
				.combinationKey(SumCombination.KEY)
				.build();
		return MeasureForest.builder()
				.name("base")
				.measure(k1Sum)
				.measure(k2Sum)
				.measure(filterK1onA1)
				.measure(k1PlusK2)
				.build();
	}

	@Test
	public void testSubForestOf_singleAggregator_noUnderlyings() {
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), List.of("k1"));

		Assertions.assertThat(sub.getNameToMeasure().keySet()).containsExactly("k1");
		Assertions.assertThat(sub.getName()).isEqualTo("base-sub");
	}

	@Test
	public void testSubForestOf_filtratorPullsItsUnderlying() {
		// `filterK1onA1` depends on `k1` — both must be kept.
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), List.of("filterK1onA1"));

		Assertions.assertThat(sub.getNameToMeasure().keySet()).containsExactlyInAnyOrder("filterK1onA1", "k1");
	}

	@Test
	public void testSubForestOf_combinatorPullsAllUnderlyings() {
		// `k1PlusK2` depends on `k1` and `k2` — all three must be kept.
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), List.of("k1PlusK2"));

		Assertions.assertThat(sub.getNameToMeasure().keySet()).containsExactlyInAnyOrder("k1PlusK2", "k1", "k2");
	}

	@Test
	public void testSubForestOf_multipleRoots_sharedUnderlyingDeduplicated() {
		// `filterK1onA1` and `k1PlusK2` both depend on `k1` — `k1` must appear exactly once in the result.
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), List.of("filterK1onA1", "k1PlusK2"));

		Assertions.assertThat(sub.getNameToMeasure().keySet())
				.containsExactlyInAnyOrder("filterK1onA1", "k1PlusK2", "k1", "k2");
		Assertions.assertThat(sub.getMeasures())
				.as("k1 must not be duplicated even though two roots depend on it")
				.hasSize(4);
	}

	@Test
	public void testSubForestOf_emptyRoots_emptyResult() {
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), List.of());

		Assertions.assertThat(sub.getMeasures()).isEmpty();
		Assertions.assertThat(sub.getName()).isEqualTo("base-sub");
	}

	@Test
	public void testSubForestOf_unknownRoot_throwsWithHint() {
		Assertions.assertThatThrownBy(() -> MeasureForestHelpers.subForestOf(baseForest(), List.of("k3")))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("forest=base")
				.hasMessageContaining("No measure named: k3");
	}

	@Test
	public void testSubForestOf_explicitName() {
		IMeasureForest sub = MeasureForestHelpers.subForestOf(baseForest(), "myCustomName", List.of("k1"));

		Assertions.assertThat(sub.getName()).isEqualTo("myCustomName");
		Assertions.assertThat(sub.getNameToMeasure().keySet()).containsExactly("k1");
	}

	@Test
	public void testSubForestOf_unresolvedTransitiveUnderlying_skipsAndContinues() {
		// `filterK1onA1` declares `underlying = "k1"`, but the source forest does NOT contain `k1`.
		// The root `filterK1onA1` resolves; the missing transitive `k1` must be skipped (warn-logged), not
		// fail the call.
		IMeasureForest forestMissingK1 = MeasureForest.builder().name("missingK1").measure(filterK1onA1).build();

		IMeasureForest sub = MeasureForestHelpers.subForestOf(forestMissingK1, List.of("filterK1onA1"));

		Assertions.assertThat(sub.getNameToMeasure().keySet()).containsExactly("filterK1onA1");
	}
}
