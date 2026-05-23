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
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.measure.operator.StandardOperatorFactory;
import eu.solven.adhoc.model.measure.Combinator;
import eu.solven.adhoc.primitive.IValueProvider;
import eu.solven.adhoc.primitive.IValueReceiver;
import eu.solven.adhoc.util.map.AdhocMapPathGet;
import lombok.extern.slf4j.Slf4j;

/**
 * Composes an ordered list of {@link ICombination}s into a single per-cell map: the first combination receives the
 * single underlying value, its result feeds the second, and so on. Used by the DAG-level linear-chain folding
 * optimization in {@code QueryStepsDagBuilder}: a chain of {@code n} single-underlying {@link Combinator}s
 * {@code A → c0 → c1 → … → cn} is rewritten to a single step that applies all {@code cᵢ}s in one pass over cells,
 * skipping the {@code n-1} intermediate cuboid materialisations.
 *
 * <p>
 * Construction reads {@value #K_CHAIN} (a {@code List<Combinator>}) and the
 * {@link StandardOperatorFactory#K_OPERATOR_FACTORY} provided by the enriching {@link IOperatorFactory}. Each
 * constituent's {@code combinationKey} + {@code combinationOptions} is resolved through the same factory at
 * construction time, so any combination type the project knows about composes transparently.
 *
 * <h3>Implementation</h3>
 *
 * <p>
 * The per-cell entry point is the primitive-friendly
 * {@link ICombination#combine(ISliceWithStep, ISlicedRecord, IValueReceiver)} shape. A pair of pre-allocated
 * {@link StageAdapter}s ping-pongs values through the chain: each adapter is both an {@link IValueReceiver} (to capture
 * a stage's output) and an {@link ISlicedRecord} (to feed it as input to the next stage). The pair is rotated so a
 * chain of arbitrary length runs with only two adapter instances per call — no per-stage {@code List.of(...)}
 * allocation, no per-stage {@code ProxyValueReceiver}, and primitive values stay on {@link IValueReceiver#onLong(long)}
 * / {@link IValueReceiver#onDouble(double)} when the constituent combinations support them.
 *
 * <p>
 * The two adapters are stored in a {@link ThreadLocal} so concurrent step evaluations on the same engine don't collide.
 * JIT escape analysis cannot eliminate the adapter allocations across the (slice, slicedRecord, receiver) call
 * boundary, hence the explicit pool.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class ComposedCombination implements ICombination {
	/** Options key carrying the ordered {@code List<Combinator>} to compose. */
	public static final String K_CHAIN = "chain";

	final List<ICombination> chain;

	// Per-thread pair of adapters. We need TWO because at each stage we read from the "current" adapter and write
	// to the "next" one — using a single instance would clobber the input before the inner combination has read it.
	private final ThreadLocal<StageAdapter[]> adapterPool = ThreadLocal.withInitial(() -> {
		StageAdapter[] pair = new StageAdapter[2];
		pair[0] = new StageAdapter();
		pair[1] = new StageAdapter();
		return pair;
	});

	public ComposedCombination(Map<String, ?> options) {
		IOperatorFactory opFactory = AdhocMapPathGet.getRequiredAs(options, StandardOperatorFactory.K_OPERATOR_FACTORY);
		List<Combinator> chainMeasures = AdhocMapPathGet.getRequiredAs(options, K_CHAIN);
		if (chainMeasures.isEmpty()) {
			throw new IllegalArgumentException("ComposedCombination requires a non-empty chain");
		}
		this.chain = chainMeasures.stream()
				.map(m -> opFactory.makeCombination(m.getCombinationKey(),
						Combinator.makeAllOptions(m, m.getCombinationOptions())))
				.toList();
	}

	@Override
	public void combine(ISliceWithStep slice, ISlicedRecord slicedRecord, IValueReceiver receiver) {
		StageAdapter[] pair = adapterPool.get();
		StageAdapter input = pair[0];
		StageAdapter output = pair[1];

		// Bootstrap: read the chain's single underlying value into `input` via the primitive path.
		input.reset();
		slicedRecord.read(0, input);

		// Walk the chain. Each inner stage reads from `input` (acts as a 1-element ISlicedRecord) and writes to
		// `output` (acts as an IValueReceiver). After each stage we swap the two so the next stage reads what the
		// previous one just wrote, and writes back into the now-stale buffer.
		int last = chain.size() - 1;
		for (int i = 0; i < last; i++) {
			output.reset();
			chain.get(i).combine(slice, input, output);
			StageAdapter tmp = input;
			input = output;
			output = tmp;
		}
		// Final stage writes directly to the caller's receiver — no intermediate adapter on the last hop.
		chain.get(last).combine(slice, input, receiver);
	}

	@Override
	public @Nullable Object combine(ISliceWithStep slice, List<?> underlyingValues) {
		// Override the boxed path too, so a caller invoking the deprecated `combine(slice, List)` route still goes
		// through our primitive composition (rather than the ICombination default, which would re-wrap into a
		// SlicedRecordFromArray + ProxyValueReceiver round-trip on top of our own).
		StageAdapter[] pair = adapterPool.get();
		StageAdapter input = pair[0];
		input.reset();
		input.onObject(underlyingValues.get(0));

		StageAdapter output = pair[1];
		int last = chain.size() - 1;
		for (int i = 0; i < last; i++) {
			output.reset();
			chain.get(i).combine(slice, input, output);
			StageAdapter tmp = input;
			input = output;
			output = tmp;
		}
		// For the last stage, capture the output through one more StageAdapter so we can extract a single value.
		StageAdapter tail = (last % 2 == 0) ? pair[1] : pair[0];
		tail.reset();
		chain.get(last).combine(slice, input, tail);
		return tail.asObject();
	}

	/**
	 * Pooled adapter that is simultaneously an {@link IValueReceiver} (captures a stage's output) and an
	 * {@link ISlicedRecord} of size 1 (feeds the captured value as input to the next stage). Internal storage avoids
	 * boxing for {@code long} / {@code double}; objects go through the generic slot.
	 */
	private static final class StageAdapter implements ISlicedRecord, IValueReceiver {
		private static final byte TYPE_OBJECT = 0;
		private static final byte TYPE_LONG = 1;
		private static final byte TYPE_DOUBLE = 2;

		private byte type;
		private long longValue;
		private double doubleValue;
		private @Nullable Object objectValue;

		void reset() {
			type = TYPE_OBJECT;
			objectValue = null;
		}

		@Nullable
		Object asObject() {
			return switch (type) {
			case TYPE_LONG -> Long.valueOf(longValue);
			case TYPE_DOUBLE -> Double.valueOf(doubleValue);
			default -> objectValue;
			};
		}

		// --- IValueReceiver: capture the previous stage's output ---

		@Override
		public void onLong(long v) {
			type = TYPE_LONG;
			longValue = v;
		}

		@Override
		public void onDouble(double v) {
			type = TYPE_DOUBLE;
			doubleValue = v;
		}

		@Override
		public void onObject(@Nullable Object v) {
			type = TYPE_OBJECT;
			objectValue = v;
		}

		// --- ISlicedRecord: replay the captured value to the next stage ---

		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			// A StageAdapter always represents a single (possibly null-valued) slot — the chain's per-cell input.
			return false;
		}

		@Override
		public void read(int index, IValueReceiver target) {
			// Single-underlying chain: every read is index 0. We don't validate to keep the hot path tight; a bug
			// in a chained combination calling read(1, ...) would surface as a primitive type mismatch downstream.
			switch (type) {
			case TYPE_LONG -> target.onLong(longValue);
			case TYPE_DOUBLE -> target.onDouble(doubleValue);
			default -> target.onObject(objectValue);
			}
		}

		@Override
		public IValueProvider read(int index) {
			// Provider-style path: rarely called by modern combinations (they prefer the receiver-style above).
			// We return a small lambda that re-routes to the receiver-style; allocation is acceptable on this cold
			// path since the chain folder targets combinations that go through the primitive path.
			return target -> read(index, target);
		}
	}
}
