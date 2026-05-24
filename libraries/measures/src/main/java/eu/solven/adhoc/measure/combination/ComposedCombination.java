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

import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ComposedCombinationPlan.CombineStep;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.primitive.IMultitypeConstants;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.map.AdhocMapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluates a {@link ComposedCombinationPlan} per cell as a single {@link ICombination}. The plan represents an
 * arbitrary expression tree (or DAG) over {@link ICombination}s; the chain case ({@code A → c0 → c1 → … → cn}) is one
 * degenerate plan and the tree case ({@code Combinator.sum(branch_a, branch_b)} where each branch is itself a chain) is
 * the general one.
 *
 * <p>
 * Used by the DAG-level subgraph-folding optimization in {@code FoldCombinatorSubgraphsOptimizer}: a connected foldable
 * subgraph of single-consumer {@link Combinator} steps is rewritten to one fused step whose combination is a
 * {@link ComposedCombination} evaluating the captured subgraph in one pass per cell, skipping every intermediate cuboid
 * materialisation.
 *
 * <p>
 * Construction reads {@value #K_PLAN} (a {@link ComposedCombinationPlan}) and the
 * {@link StandardOperatorFactory#K_OPERATOR_FACTORY} provided by the enriching {@link IOperatorFactory}. Each step's
 * {@code combinator} is resolved to an {@link ICombination} through the same factory at construction time, so any
 * combination type the project knows about composes transparently.
 *
 * <h3>Implementation</h3>
 *
 * <p>
 * The per-cell entry point is the primitive-friendly
 * {@link ICombination#combine(ISliceWithStep, ISlicedRecord, IValueReceiver)} shape. A per-thread pool holds one
 * {@link StageAdapter} per plan slot (each adapter is both an {@link IValueReceiver} that captures a value and a
 * single-slot {@link ISlicedRecord} that exposes it) plus one {@link MultiSlotRecord} per combine step (a fixed view
 * over the step's input slots). At evaluation time we copy the input record into the leaf slots, run each combine step
 * writing to its scratch slot, and stream the root step's output directly into the caller's receiver — no intermediate
 * {@code List.of(...)} allocation, no per-step {@code ProxyValueReceiver}, and primitive values stay on
 * {@link IValueReceiver#onLong(long)} / {@link IValueReceiver#onDouble(double)} when constituent combinations support
 * them.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class ComposedCombination implements ICombination {

	/**
	 * Short combination-key registered in {@link StandardOperatorFactory}. Used by
	 * {@link eu.solven.adhoc.engine.optimizer.FoldCombinatorSubgraphsOptimizer} when assembling the fused step, so
	 * EXPLAIN traces render {@code Combinator[COMPOSED]} rather than the fully-qualified class name.
	 */
	public static final String KEY = "COMPOSED";

	/**
	 * Options key carrying the {@link ComposedCombinationPlan} to evaluate.
	 */
	public static final String K_PLAN = "plan";

	private final ComposedCombinationPlan plan;
	private final ICombination[] stepCombinations;

	// Per-thread scratch: one StageAdapter per plan slot + one MultiSlotRecord per combine step (each pre-bound
	// to its inputSlots). Created lazily on first use and rotated via ThreadLocal so concurrent step evaluations on
	// the same ComposedCombination instance don't collide. JIT escape analysis cannot collapse these allocations
	// across the (slice, slicedRecord, receiver) call boundary, hence the explicit pool.
	private final ThreadLocal<EvalContext> contextPool;

	public ComposedCombination(Map<String, ?> options) {
		IOperatorFactory opFactory = AdhocMapPathGet.getRequiredAs(options, StandardOperatorFactory.K_OPERATOR_FACTORY);
		this.plan = AdhocMapPathGet.getRequiredAs(options, K_PLAN);

		// Resolve each step's combinator to a concrete ICombination via the operator factory. We hold the resolved
		// instances in a parallel array so evaluation can index them positionally.
		List<CombineStep> steps = plan.steps();
		this.stepCombinations = new ICombination[steps.size()];
		for (int i = 0; i < steps.size(); i++) {
			Combinator combinator = steps.get(i).combinator();
			this.stepCombinations[i] = opFactory.makeCombination(combinator.getCombinationKey(),
					Combinator.makeAllOptions(combinator, combinator.getCombinationOptions()));
		}

		this.contextPool = ThreadLocal.withInitial(() -> new EvalContext(plan));
	}

	@Override
	public void combine(ISliceWithStep slice, ISlicedRecord slicedRecord, IValueReceiver receiver) {
		EvalContext ctx = contextPool.get();
		StageAdapter[] slots = ctx.slots;
		MultiSlotRecord[] views = ctx.views;

		// Fill leaf slots from the input record.
		int numLeaves = plan.numLeaves();
		for (int i = 0; i < numLeaves; i++) {
			slots[i].reset();
			slicedRecord.read(i, slots[i]);
		}

		// Run each combine step except the last, writing into its scratch slot.
		List<CombineStep> steps = plan.steps();
		int last = steps.size() - 1;
		for (int k = 0; k < last; k++) {
			StageAdapter target = slots[numLeaves + k];
			target.reset();
			stepCombinations[k].combine(slice, views[k], target);
		}
		// Final step writes directly to the caller's receiver — saves one StageAdapter copy on the last hop.
		stepCombinations[last].combine(slice, views[last], receiver);
	}

	@Override
	public @Nullable Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		// Override the boxed path so callers using the deprecated `combine(slice, List)` route still benefit from
		// the primitive composition. We buffer the boxed inputs into the leaf StageAdapters, run the plan, then
		// extract the root value via a final scratch capture.
		EvalContext ctx = contextPool.get();
		StageAdapter[] slots = ctx.slots;
		MultiSlotRecord[] views = ctx.views;

		int numLeaves = plan.numLeaves();
		for (int i = 0; i < numLeaves; i++) {
			slots[i].reset();
			slots[i].onObject(underlyingValues.get(i));
		}

		List<CombineStep> steps = plan.steps();
		int last = steps.size() - 1;
		for (int k = 0; k < last; k++) {
			StageAdapter target = slots[numLeaves + k];
			target.reset();
			stepCombinations[k].combine(slice, views[k], target);
		}
		StageAdapter rootTarget = slots[numLeaves + last];
		rootTarget.reset();
		stepCombinations[last].combine(slice, views[last], rootTarget);
		return rootTarget.asObject();
	}

	/** Per-thread evaluation buffers. Lazily allocated once per thread per ComposedCombination instance. */
	private static final class EvalContext {
		final StageAdapter[] slots;
		final MultiSlotRecord[] views;

		EvalContext(ComposedCombinationPlan plan) {
			int total = plan.totalSlots();
			this.slots = new StageAdapter[total];
			for (int i = 0; i < total; i++) {
				slots[i] = new StageAdapter();
			}
			List<CombineStep> steps = plan.steps();
			this.views = new MultiSlotRecord[steps.size()];
			for (int k = 0; k < steps.size(); k++) {
				views[k] = new MultiSlotRecord(slots, steps.get(k).inputSlots());
			}
		}
	}

	/**
	 * View of selected slots as an {@link ISlicedRecord}. Pre-bound at construction to the step's {@code inputSlots},
	 * so evaluation is one array lookup per position with no allocation on the hot path.
	 */
	private static final class MultiSlotRecord implements ISlicedRecord {
		private final StageAdapter[] allSlots;
		private final int[] selected;

		// Internal pooled adapter — both arrays are owned by the enclosing EvalContext, never escape; defensive copy
		// would defeat the per-thread reuse this class exists for. Last parameter stays a plain int[] for the same
		// reason (callers build it via Arrays.copyOf, not literal list).
		@SuppressWarnings({ "PMD.ArrayIsStoredDirectly", "PMD.UseVarargs" })
		MultiSlotRecord(StageAdapter[] allSlots, int[] selected) {
			this.allSlots = allSlots;
			this.selected = selected;
		}

		@Override
		public int size() {
			return selected.length;
		}

		@Override
		public boolean isEmpty() {
			return selected.length == 0;
		}

		@Override
		public void read(int index, IValueReceiver target) {
			allSlots[selected[index]].read(0, target);
		}

		@Override
		public IValueProvider read(int index) {
			// Cold path; modern combinations use the receiver-style above.
			int slot = selected[index];
			return target -> allSlots[slot].read(0, target);
		}
	}

	/**
	 * Pooled single-slot adapter that is simultaneously an {@link IValueReceiver} (captures a stage's output) and an
	 * {@link ISlicedRecord} of size 1 (feeds the captured value as input to a downstream stage). Internal storage
	 * avoids boxing for {@code long} / {@code double}; objects go through the generic slot.
	 */
	// TODO Similar with MultitypeCell
	private static final class StageAdapter implements ISlicedRecord, IValueReceiver {

		private byte type;
		private long longValue;
		private double doubleValue;
		private @Nullable Object objectValue;

		// Null-assignment is the only way to release the previous reference so the GC can reclaim it across pool reuse.
		@SuppressWarnings("PMD.NullAssignment")
		void reset() {
			type = IMultitypeConstants.MASK_OBJECT;
			objectValue = null;
		}

		@Nullable
		Object asObject() {
			return switch (type) {
			case IMultitypeConstants.MASK_LONG -> longValue;
			case IMultitypeConstants.MASK_DOUBLE -> doubleValue;
			default -> objectValue;
			};
		}

		// --- IValueReceiver: capture the previous stage's output ---

		@Override
		public void onLong(long v) {
			type = IMultitypeConstants.MASK_LONG;
			longValue = v;
		}

		@Override
		public void onDouble(double v) {
			type = IMultitypeConstants.MASK_DOUBLE;
			doubleValue = v;
		}

		@Override
		public void onObject(@Nullable Object v) {
			type = IMultitypeConstants.MASK_OBJECT;
			objectValue = v;
		}

		// --- ISlicedRecord: replay the captured value to the next stage ---

		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void read(int index, IValueReceiver target) {
			switch (type) {
			case IMultitypeConstants.MASK_LONG -> target.onLong(longValue);
			case IMultitypeConstants.MASK_DOUBLE -> target.onDouble(doubleValue);
			default -> target.onObject(objectValue);
			}
		}

		@Override
		public IValueProvider read(int index) {
			return target -> read(index, target);
		}
	}
}
