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
package eu.solven.adhoc.dataframe.column;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import eu.solven.adhoc.collection.ICompactable;
import eu.solven.adhoc.dataframe.aggregating.AggregatingColumnsDistinct;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashIntColumn;
import eu.solven.adhoc.dataframe.column.hash.MultitypeHashMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableIntColumn;
import eu.solven.adhoc.dataframe.column.navigable.MultitypeNavigableMergeableColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashColumn;
import eu.solven.adhoc.dataframe.column.navigable_else_hash.MultitypeNavigableElseHashMergeableColumn;
import eu.solven.adhoc.engine.step.CubeQueryStep;
import eu.solven.adhoc.measure.aggregation.IAggregation;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.sum.SumAggregation;
import lombok.extern.slf4j.Slf4j;

/**
 * JOL-backed footprint comparison across the different {@code Multitype*Column} variants when holding an equivalent
 * {@code N}-slice cuboid. Produces human-readable {@code bytes/slice} figures in the test log and sanity-checks every
 * measurement against a generous upper bound.
 *
 * <p>
 * Tests are executed under the parent POM's default {@code -Xmx512M} surefire heap, which is sized to fit
 * {@code N = 200_000} comfortably for every variant. The constant can be raised locally (e.g. by exporting
 * {@code MAVEN_OPTS=-Xmx4g}) to reproduce the million-slice scale the roadmap entry targets.
 *
 * <p>
 * The {@code testFootprint_AggregatingColumnsDistinct} case is the realistic end-to-end scenario: a two-dimensional
 * cuboid built from a {@link ColorIndexSlice} record (a Comparable stand-in for a {@code Map<String, Object>} slice,
 * since {@link AggregatingColumnsDistinct} requires a {@code Comparable} key type). The other cases drop the
 * dictionarizing front-end entirely and populate a column directly with {@link Integer} (or primitive {@code int}) keys
 * to isolate the storage-layer cost.
 *
 * @author Benoit Lacelle
 */
@Slf4j
public class TestColumnFootprint {

	/**
	 * Number of distinct slices populated into each column under test. Calibrated so the heaviest variant
	 * ({@link AggregatingColumnsDistinct} with {@link ColorIndexSlice} slices) still fits in the default
	 * {@code -Xmx512M} surefire heap. Raise locally to stress.
	 */
	private static final int N = 200_000;

	private static final Aggregator SUM_A = Aggregator.sum("a");
	private static final IAggregation SUM_AGG = new SumAggregation();
	private static final CubeQueryStep STEP = CubeQueryStep.builder().measure("a").build();

	/**
	 * Comparable stand-in for a two-dimensional {@code Map<String, Object>} slice: carries the
	 * {@code color} (blue/red) and {@code rawIndex} dimensions the roadmap entry names. Implemented as a
	 * {@code record} because {@link AggregatingColumnsDistinct} requires {@code T extends Comparable<T>} and a
	 * raw {@link java.util.Map} is not {@link Comparable}.
	 */
	private record ColorIndexSlice(String color, int rawIndex) implements Comparable<ColorIndexSlice> {

	@Override
	public int compareTo(ColorIndexSlice o) {
		int byColor = color.compareTo(o.color);
		if (byColor != 0) {
			return byColor;
		} else {
			return Integer.compare(rawIndex, o.rawIndex);
		}
	}

	}

	@Test
	public void testFootprint_MultitypeHashColumn() {
		MultitypeHashColumn<Integer> col = MultitypeHashColumn.<Integer>builder().build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeHashColumn<Integer>", col, 28_366_248L, 9_491_848L);
	}

	@Test
	public void testFootprint_MultitypeHashMergeableColumn() {
		MultitypeHashMergeableColumn<Integer> col =
				MultitypeHashMergeableColumn.<Integer>builder().aggregation(SUM_AGG).build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeHashMergeableColumn<Integer>", col, 28_366_264L, 9_491_864L);
	}

	@Test
	public void testFootprint_MultitypeHashIntColumn() {
		MultitypeHashIntColumn col = MultitypeHashIntColumn.builder().build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeHashIntColumn", col, 25_166_248L, 6_291_848L);
	}

	@Test
	public void testFootprint_MultitypeNavigableColumn() {
		MultitypeNavigableColumn<Integer> col = MultitypeNavigableColumn.<Integer>builder().build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeNavigableColumn<Integer>", col, 6_346_576L, 5_849_424L);
	}

	@Test
	public void testFootprint_MultitypeNavigableMergeableColumn() {
		MultitypeNavigableMergeableColumn<Integer> col =
				MultitypeNavigableMergeableColumn.<Integer>builder().aggregation(SUM_AGG).build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeNavigableMergeableColumn<Integer>", col, 6_346_600L, 5_849_448L);
	}

	@Test
	public void testFootprint_MultitypeNavigableIntColumn() {
		MultitypeNavigableIntColumn col = MultitypeNavigableIntColumn.builder().build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeNavigableIntColumn", col, 6_097_688L, 2_400_536L);
	}

	@Test
	public void testFootprint_MultitypeNavigableElseHashColumn() {
		MultitypeNavigableElseHashColumn<Integer> col = MultitypeNavigableElseHashColumn.<Integer>builder().build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeNavigableElseHashColumn<Integer>", col, 6_347_048L, 5_849_848L);
	}

	@Test
	public void testFootprint_MultitypeNavigableElseHashMergeableColumn() {
		MultitypeNavigableElseHashMergeableColumn<Integer> col =
				MultitypeNavigableElseHashMergeableColumn.<Integer>builder().aggregation(SUM_AGG).build();
		for (int i = 0; i < N; i++) {
			col.append(i).onLong(i);
		}
		assertFootprint("MultitypeNavigableElseHashMergeableColumn<Integer>", col, 6_347_064L, 5_849_864L);
	}

	@Test
	public void testFootprint_AggregatingColumnsDistinct() {
		AggregatingColumnsDistinct<ColorIndexSlice> agg = AggregatingColumnsDistinct.<ColorIndexSlice>builder().build();
		for (int i = 0; i < N; i++) {
			ColorIndexSlice slice = new ColorIndexSlice(i % 2 == 0 ? "blue" : "red", i);
			agg.contribute(slice, SUM_A).onLong(i);
		}

		IMultitypeColumnFastGet<ColorIndexSlice> closed = agg.closeColumn(STEP, SUM_A);
		assertFootprint("AggregatingColumnsDistinct<ColorIndexSlice>/closed", closed, 12_195_568L, 12_195_568L);
	}

	/**
	 * Two-phase footprint check:
	 * <ol>
	 * <li>Measure the populated {@code instance} and assert against {@code expectedBytes} (pre-compact baseline).</li>
	 * <li>Invoke {@link ICompactable#compact()} when {@code instance} implements it, then re-measure and assert against
	 * {@code expectedCompactedBytes} (post-compact baseline). Callers pass the <em>same</em> value for both baselines
	 * when the variant is already tight or not {@link ICompactable}.</li>
	 * </ol>
	 *
	 * <p>
	 * Both bounds are calibrated to the layout observed on the reference JDK (JDK 25, default compressed oops, fastutil
	 * 8.x, {@code N = 200_000}) so any change — a new field, a different backing collection, a capacity tweak — fails
	 * the test and forces a deliberate baseline update. A small ±0.5% tolerance absorbs non-semantic perturbations
	 * (e.g. a different {@code capacity} hint rounding to the same power-of-two) without masking meaningful
	 * regressions.
	 *
	 * @param name
	 *            a short label for the column variant, included in the log lines and the assertion-failure description.
	 * @param instance
	 *            the populated column (or any JOL-walkable object graph) to measure.
	 * @param expectedBytes
	 *            the baseline {@code GraphLayout.parseInstance(instance).totalSize()} value observed right after
	 *            population, before {@code compact()}. Update deliberately when a refactor changes the layout on
	 *            purpose.
	 * @param expectedCompactedBytes
	 *            the baseline value observed after {@code compact()} has run. When {@code instance} is not
	 *            {@link ICompactable}, pass the same value as {@code expectedBytes} (compact is a no-op).
	 */
	private static void assertFootprint(String name, Object instance, long expectedBytes, long expectedCompactedBytes) {
		assertSize(name + " (populated)", instance, expectedBytes);

		if (instance instanceof ICompactable compactable) {
			compactable.compact();
			assertSize(name + " (compacted)", instance, expectedCompactedBytes);
		} else {
			Assertions.assertThat(expectedCompactedBytes)
					.as("%s is not ICompactable — callers must pass expectedCompactedBytes == expectedBytes", name)
					.isEqualTo(expectedBytes);
		}
	}

	/**
	 * Walks {@code instance} via JOL, logs the total retained footprint, and asserts an exact baseline match with a
	 * ±0.5% tolerance. Used for each of the two phases in {@link #assertFootprint}.
	 *
	 * @param phaseLabel
	 *            the variant label qualified with the phase (populated / compacted), for the log line.
	 * @param instance
	 *            the JOL-walkable object graph to measure.
	 * @param expectedBytes
	 *            the baseline retained-size value.
	 */
	private static void assertSize(String phaseLabel, Object instance, long expectedBytes) {
		long size = GraphLayout.parseInstance(instance).totalSize();
		double bytesPerSlice = size / (double) N;
		log.info("[footprint] {} N={} totalSize={} bytes ({} bytes/slice) baseline={}",
				phaseLabel,
				N,
				size,
				bytesPerSlice,
				expectedBytes);

		Assertions.assertThat(size)
				.as("%s footprint (N=%d) — update baseline if the change is deliberate", phaseLabel, N)
				.isCloseTo(expectedBytes, Offset.offset((long) Math.ceil(expectedBytes * 0.005)));
	}
}
