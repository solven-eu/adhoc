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
package eu.solven.adhoc.measure.combination;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.model.measure.Combinator;

/**
 * Flat slot-based representation of the expression tree a {@link ComposedCombination} evaluates per cell.
 *
 * <p>
 * Slot numbering:
 * <ul>
 * <li>{@code 0 .. numLeaves - 1} — input slots. Slot {@code i} is populated by reading position {@code i} of the
 * per-cell {@code ISlicedRecord} the engine hands to {@link ComposedCombination#combine}.</li>
 * <li>{@code numLeaves .. numLeaves + steps.size() - 1} — scratch slots. The {@link CombineStep} at position {@code k}
 * in {@code steps} writes its output into scratch slot {@code numLeaves + k}.</li>
 * </ul>
 *
 * <p>
 * Steps are in evaluation order (any post-order traversal of the expression tree works, as long as a step's
 * {@code inputSlots} all reference earlier slots). The final step's output is the value of the whole expression and is
 * forwarded directly to the caller's {@code IValueReceiver}, with no intermediate copy.
 *
 * <p>
 * A linear chain {@code A → c0 → c1 → c2} (the original chain-folding case) is one degenerate plan:
 * {@code numLeaves = 1}, three single-input combine steps, each reading the previous step's output slot. A
 * multi-underlying tree like {@code root_C(a_C(A), b_C(B))} has {@code numLeaves = 2}, three combine steps; the root's
 * {@code inputSlots} reference the two intermediate slots produced by {@code a_C} and {@code b_C}. Shared boundary
 * leaves (e.g. when both subtrees read the same aggregator) appear once and are referenced by index from multiple
 * combine steps.
 *
 * @author Benoit Lacelle
 */
public record ComposedCombinationPlan(int numLeaves, List<CombineStep> steps) {

	/**
	 * Per-step entry in the plan. {@code combinator} is the per-cell map applied to the values at {@code inputSlots}.
	 */
	public record CombineStep(Combinator combinator, int[] inputSlots) {
		public CombineStep {
			if (combinator == null) {
				throw new IllegalArgumentException("combinator must not be null");
			}
			if (inputSlots == null || inputSlots.length == 0) {
				throw new IllegalArgumentException("inputSlots must be non-empty");
			}
		}

		// The auto-generated record accessors / equals / hashCode / toString use Object.equals / Object.hashCode /
		// Object.toString on `inputSlots`, which compare and render by reference identity — wrong for an int[] payload.
		// Override all three with array-aware semantics so equal plans actually compare equal and EXPLAIN prints the
		// slot indices instead of `[I@1234abcd`.
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof CombineStep other)) {
				return false;
			}
			return Objects.equals(combinator, other.combinator) && Arrays.equals(inputSlots, other.inputSlots);
		}

		@Override
		public int hashCode() {
			return Objects.hash(combinator, Arrays.hashCode(inputSlots));
		}

		@Override
		public String toString() {
			return "CombineStep[combinator=" + combinator + ", inputSlots=" + Arrays.toString(inputSlots) + "]";
		}
	}

	public ComposedCombinationPlan {
		if (numLeaves < 0) {
			throw new IllegalArgumentException("numLeaves must be >= 0, got: " + numLeaves);
		}
		steps = ImmutableList.copyOf(steps);
		if (steps.isEmpty()) {
			throw new IllegalArgumentException("steps must be non-empty");
		}
		// Validate slot references: every inputSlot must be an earlier slot (either a leaf or a previously-produced
		// scratch slot). This catches plans built incorrectly at construction rather than at evaluation time.
		int total = numLeaves + steps.size();
		for (int k = 0; k < steps.size(); k++) {
			int producedSlot = numLeaves + k;
			for (int input : steps.get(k).inputSlots()) {
				if (input < 0 || input >= producedSlot) {
					throw new IllegalArgumentException("CombineStep[" + k
							+ "] references slot "
							+ input
							+ " which is not strictly earlier than its own target slot "
							+ producedSlot
							+ " (total slots: "
							+ total
							+ ")");
				}
			}
		}
	}

	/** @return total number of slots the evaluator must allocate. */
	public int totalSlots() {
		return numLeaves + steps.size();
	}

	/** @return slot index where the final result lands (the last combine step's target). */
	public int rootSlot() {
		return numLeaves + steps.size() - 1;
	}
}