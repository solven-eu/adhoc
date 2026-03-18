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
package eu.solven.adhoc.measure.forest;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.filter.AndFilter;
import eu.solven.adhoc.filter.ColumnFilter;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.filter.NotFilter;
import eu.solven.adhoc.filter.OrFilter;
import eu.solven.adhoc.measure.forest.MeasureForestConcealer.ConcealingDefinition;
import eu.solven.adhoc.measure.forest.MeasureForestConcealer.ConcealingResult;
import eu.solven.adhoc.measure.model.Aggregator;
import eu.solven.adhoc.measure.model.Columnator;
import eu.solven.adhoc.measure.model.Combinator;
import eu.solven.adhoc.measure.model.Dispatchor;
import eu.solven.adhoc.measure.model.Filtrator;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.model.Partitionor;
import eu.solven.adhoc.measure.model.Shiftor;

public class TestMeasureForestConcealer {

	final MeasureForestConcealer concealer = new MeasureForestConcealer();

	// ── helpers ──────────────────────────────────────────────────────────────

	/** Returns the concealed name that the given original name maps to (m_ prefix). */
	private static String mn(String originalName) {
		return "m_" + String.format("%08x", originalName.hashCode() & 0xFFFFFFFFL);
	}

	/** Returns the concealed column name (c_ prefix). */
	private static String cn(String originalColumn) {
		return "c_" + String.format("%08x", originalColumn.hashCode() & 0xFFFFFFFFL);
	}

	/** Returns the concealed tag (t_ prefix). */
	private static String tn(String originalTag) {
		return "t_" + String.format("%08x", originalTag.hashCode() & 0xFFFFFFFFL);
	}

	// ── Aggregator ────────────────────────────────────────────────────────────

	// The Aggregator type is preserved. The measure name and columnName are both hashed.
	// aggregationKey and aggregationOptions are kept as-is.
	@Test
	public void testAggregator_nameAndColumnConcealed_keyPreserved() {
		Aggregator secret = Aggregator.builder()
				.name("revenue")
				.columnName("raw_revenue_col")
				.aggregationKey("customAggKey")
				.build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("revenue");
		IMeasure result = concealed.getNameToMeasure().get(mn("revenue"));
		Assertions.assertThat(result).isInstanceOf(Aggregator.class);

		Aggregator concealedAgg = (Aggregator) result;
		Assertions.assertThat(concealedAgg.getName()).isEqualTo(mn("revenue"));
		Assertions.assertThat(concealedAgg.getColumnName()).isEqualTo(cn("raw_revenue_col"));
		// aggregation key preserved
		Assertions.assertThat(concealedAgg.getAggregationKey()).isEqualTo("customAggKey");
		// no options were set, none appear
		Assertions.assertThat(concealedAgg.getAggregationOptions()).isEmpty();
	}

	// ── Combinator ────────────────────────────────────────────────────────────

	// The Combinator type, combinationKey, and combinationOptions are all preserved.
	// Only the name and underlying references are hashed.
	@Test
	public void testCombinator_nameAndUnderlyingsConcealed_formulaPreserved() {
		Combinator secret = Combinator.builder()
				.name("ratio")
				.underlying("numerator")
				.underlying("denominator")
				.combinationKey("customDivideKey")
				.combinationOption("scale", 4)
				.build();
		IMeasureForest forest = MeasureForest.builder()
				.name("myForest")
				.measure(Aggregator.sum("numerator"))
				.measure(Aggregator.sum("denominator"))
				.measure(secret)
				.build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("ratio");
		IMeasure result = concealed.getNameToMeasure().get(mn("ratio"));
		Assertions.assertThat(result).isInstanceOf(Combinator.class);

		Combinator concealedComb = (Combinator) result;
		Assertions.assertThat(concealedComb.getName()).isEqualTo(mn("ratio"));
		// underlyings renamed to hashed measure names
		Assertions.assertThat(concealedComb.getUnderlyings()).containsExactly(mn("numerator"), mn("denominator"));
		// combinationKey preserved; options cleared
		Assertions.assertThat(concealedComb.getCombinationKey()).isEqualTo("customDivideKey");
		Assertions.assertThat(concealedComb.getCombinationOptions()).isEmpty();
	}

	// ── Filtrator ────────────────────────────────────────────────────────────

	// The Filtrator type is preserved. Name and underlying reference are hashed.
	// The filter's column name is also hashed.
	@Test
	public void testFiltrator_nameUnderlyingAndFilterColumnConcealed() {
		Filtrator secret = Filtrator.builder()
				.name("revenueEur")
				.underlying("revenue")
				.filter(ColumnFilter.matchEq("ccy", "EUR"))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("revenueEur");
		IMeasure result = concealed.getNameToMeasure().get(mn("revenueEur"));
		Assertions.assertThat(result).isInstanceOf(Filtrator.class);

		Filtrator concealedFil = (Filtrator) result;
		Assertions.assertThat(concealedFil.getName()).isEqualTo(mn("revenueEur"));
		Assertions.assertThat(concealedFil.getUnderlying()).isEqualTo(mn("revenue"));
		// column name in the filter is concealed
		Assertions.assertThat(concealedFil.getFilter()).isEqualTo(ColumnFilter.matchEq(cn("ccy"), "EUR"));
	}

	// Filtrator with a composite filter: AND(col1=v1, OR(col2=v2), NOT(col3=v3)).
	// All column names must be hashed; values and logical structure are preserved.
	@Test
	public void testFiltrator_compositeFilter_allColumnsConcealed() {
		ISliceFilter compositeFilter = AndFilter.and(ColumnFilter.matchEq("col1", "v1"),
				OrFilter.or(Map.of("col2", "v2")),
				NotFilter.builder().negated(ColumnFilter.matchEq("col3", "v3")).build());
		Filtrator secret = Filtrator.builder().name("filtered").underlying("base").filter(compositeFilter).build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("base")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Filtrator concealedFil = (Filtrator) concealed.getNameToMeasure().get(mn("filtered"));
		// All columns in the concealed filter start with the c_ prefix.
		Set<String> concealedCols = new LinkedHashSet<>();
		MeasureForestConcealer.collectFilterColumns(concealedFil.getFilter(), concealedCols);
		Assertions.assertThat(concealedCols).allMatch(c -> c.startsWith("c_"));
	}

	// ── Shiftor ───────────────────────────────────────────────────────────────

	// The Shiftor type, editorKey, and editorOptions are preserved (options are not modified by default).
	// Name and underlying are hashed.
	@Test
	public void testShiftor_nameAndUnderlyingConcealed_optionsPreserved() {
		Shiftor secret = Shiftor.builder()
				.name("shiftedRevenue")
				.underlying("revenue")
				.editorKey("someEditorKey")
				.editorOption("country", "FR")
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("shiftedRevenue");
		IMeasure result = concealed.getNameToMeasure().get(mn("shiftedRevenue"));
		Assertions.assertThat(result).isInstanceOf(Shiftor.class);

		Shiftor concealedShiftor = (Shiftor) result;
		Assertions.assertThat(concealedShiftor.getName()).isEqualTo(mn("shiftedRevenue"));
		Assertions.assertThat(concealedShiftor.getUnderlying()).isEqualTo(mn("revenue"));
		// editorKey preserved (not a column or value); editorOptions cleared
		Assertions.assertThat(concealedShiftor.getEditorKey()).isEqualTo("someEditorKey");
		Assertions.assertThat(concealedShiftor.getEditorOptions()).isEmpty();
	}

	// ── Tags ──────────────────────────────────────────────────────────────────

	// Tags on each measure are hashed with the t_ prefix.
	@Test
	public void testAggregator_tagsConcealed() {
		Aggregator secret = Aggregator.builder().name("revenue").columnName("col").tag("pnl").tag("team-a").build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Aggregator concealedAgg = (Aggregator) concealed.getNameToMeasure().get(mn("revenue"));
		Assertions.assertThat(concealedAgg.getTags()).containsExactlyInAnyOrder(tn("pnl"), tn("team-a"));
	}

	// ── Two-level forest ──────────────────────────────────────────────────────

	// Nominal: aggregator + combinator referencing it — both names and the underlying reference are hashed.
	@Test
	public void testForest_twoLevels_namesAndRefsConcealed() {
		Aggregator leaf = Aggregator.builder().name("rawRevenue").columnName("secret_col").build();
		Combinator parent = Combinator.builder()
				.name("adjustedRevenue")
				.underlying("rawRevenue")
				.combinationKey("secretFormula")
				.build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(leaf).measure(parent).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).containsOnlyKeys(mn("rawRevenue"), mn("adjustedRevenue"));

		Aggregator concealedLeaf = (Aggregator) concealed.getNameToMeasure().get(mn("rawRevenue"));
		Assertions.assertThat(concealedLeaf.getColumnName()).isEqualTo(cn("secret_col"));

		Combinator concealedParent = (Combinator) concealed.getNameToMeasure().get(mn("adjustedRevenue"));
		// underlying reference points to the hashed leaf name
		Assertions.assertThat(concealedParent.getUnderlyings()).containsExactly(mn("rawRevenue"));
		// formula preserved
		Assertions.assertThat(concealedParent.getCombinationKey()).isEqualTo("secretFormula");
	}

	// ── Forest name ───────────────────────────────────────────────────────────

	@Test
	public void testForestName_isHashed() {
		IMeasureForest forest = MeasureForest.builder().name("secretForest").build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getName()).isEqualTo(mn("secretForest"));
	}

	// ── Collision handling ────────────────────────────────────────────────────

	// "Aa" and "BB" have identical Java hashCode (2112 = 0x840).
	// The first entry in iteration order wins the un-suffixed slot; the second gets _2.
	@Test
	public void testHashCollision_suffixApplied() {
		// Use LinkedHashSet to guarantee iteration order: "Aa" first, "BB" second.
		Set<String> names = new LinkedHashSet<>();
		names.add("Aa");
		names.add("BB");

		Map<String, String> mapping = MeasureForestConcealer.buildMapping("m_", names);

		Assertions.assertThat(mapping.get("Aa")).isEqualTo("m_00000840");
		Assertions.assertThat(mapping.get("BB")).isEqualTo("m_00000840_2");
	}

	// ── Original forest is not mutated ────────────────────────────────────────

	@Test
	public void testOriginalForestUnchanged() {
		Aggregator secret = Aggregator.builder().name("revenue").columnName("raw_col").aggregationKey("max").build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(secret).build();

		concealer.conceal(forest);

		Aggregator original = (Aggregator) forest.getNameToMeasure().get("revenue");
		Assertions.assertThat(original.getColumnName()).isEqualTo("raw_col");
		Assertions.assertThat(original.getAggregationKey()).isEqualTo("max");
	}

	// ── Restoration ──────────────────────────────────────────────────────────

	// After concealment + restoration the forest is equal to the original.
	@Test
	public void testRestoration_roundtrip() {
		Aggregator leaf = Aggregator.builder().name("rawRevenue").columnName("secret_col").tag("pnl").build();
		Filtrator filtered = Filtrator.builder()
				.name("eurRevenue")
				.underlying("rawRevenue")
				.filter(ColumnFilter.matchEq("ccy", "EUR"))
				.build();
		IMeasureForest original = MeasureForest.builder().name("myForest").measure(leaf).measure(filtered).build();

		ConcealingResult result = concealer.concealWithDefinition(original);
		ConcealingDefinition definition = result.getDefinition();

		IMeasureForest restored = concealer.restore(result.getConcealedForest(), definition);

		Assertions.assertThat(restored.getName()).isEqualTo("myForest");
		Assertions.assertThat(restored.getNameToMeasure()).containsOnlyKeys("rawRevenue", "eurRevenue");

		Aggregator restoredLeaf = (Aggregator) restored.getNameToMeasure().get("rawRevenue");
		Assertions.assertThat(restoredLeaf.getColumnName()).isEqualTo("secret_col");
		Assertions.assertThat(restoredLeaf.getTags()).containsExactly("pnl");

		Filtrator restoredFil = (Filtrator) restored.getNameToMeasure().get("eurRevenue");
		Assertions.assertThat(restoredFil.getUnderlying()).isEqualTo("rawRevenue");
		Assertions.assertThat(restoredFil.getFilter()).isEqualTo(ColumnFilter.matchEq("ccy", "EUR"));
	}

	// ── Columnator ────────────────────────────────────────────────────────────

	// The Columnator type, mode, combinationKey, and combinationOptions are preserved.
	// Name, underlyings, and columns are all concealed.
	@Test
	public void testColumnator_nameUnderlyingsAndColumnsConcealed() {
		Columnator secret = Columnator.builder()
				.name("conditionalRevenue")
				.underlying("revenue")
				.column("ccy")
				.column("country")
				.mode(Columnator.Mode.HideIfMissing)
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("conditionalRevenue");
		IMeasure result = concealed.getNameToMeasure().get(mn("conditionalRevenue"));
		Assertions.assertThat(result).isInstanceOf(Columnator.class);

		Columnator concealedCol = (Columnator) result;
		Assertions.assertThat(concealedCol.getName()).isEqualTo(mn("conditionalRevenue"));
		// underlying measure reference is hashed
		Assertions.assertThat(concealedCol.getUnderlyings()).containsExactly(mn("revenue"));
		// column names are hashed
		Assertions.assertThat(concealedCol.getColumns()).containsExactlyInAnyOrder(cn("ccy"), cn("country"));
		// mode and combinationKey preserved
		Assertions.assertThat(concealedCol.getMode()).isEqualTo(Columnator.Mode.HideIfMissing);
		Assertions.assertThat(concealedCol.getCombinationKey()).isNotEmpty();
	}

	// ── Partitionor ───────────────────────────────────────────────────────────

	// The Partitionor type, aggregationKey, combinationKey, and options are preserved.
	// Name, underlyings, and groupBy column names are all concealed.
	@Test
	public void testPartitionor_groupByColumnsConcealed() {
		Partitionor secret = Partitionor.builder()
				.name("fxRevenue")
				.underlying("revenue")
				.groupBy(eu.solven.adhoc.query.groupby.GroupByColumns.named("ccy", "region"))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("fxRevenue");
		IMeasure result = concealed.getNameToMeasure().get(mn("fxRevenue"));
		Assertions.assertThat(result).isInstanceOf(Partitionor.class);

		Partitionor concealedPar = (Partitionor) result;
		Assertions.assertThat(concealedPar.getName()).isEqualTo(mn("fxRevenue"));
		// underlying measure reference is hashed
		Assertions.assertThat(concealedPar.getUnderlyings()).containsExactly(mn("revenue"));
		// groupBy column names are hashed
		Assertions.assertThat(concealedPar.getGroupBy().getGroupedByColumns())
				.containsExactlyInAnyOrder(cn("ccy"), cn("region"));
		// aggregation and combination keys preserved
		Assertions.assertThat(concealedPar.getAggregationKey()).isNotEmpty();
		Assertions.assertThat(concealedPar.getCombinationKey()).isNotEmpty();
	}

	// ── Dispatchor ────────────────────────────────────────────────────────────

	// The Dispatchor type, aggregationKey, and decompositionKey are preserved.
	// Name and underlying are concealed; aggregationOptions and decompositionOptions are cleared.
	@Test
	public void testDispatchor_optionsCleared() {
		Dispatchor secret = Dispatchor.builder()
				.name("dispatched")
				.underlying("revenue")
				.decompositionKey("percentBucket")
				.decompositionOption("min", 0)
				.decompositionOption("max", 100)
				.aggregationOption("scale", 2)
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Assertions.assertThat(concealed.getNameToMeasure()).doesNotContainKey("dispatched");
		IMeasure result = concealed.getNameToMeasure().get(mn("dispatched"));
		Assertions.assertThat(result).isInstanceOf(Dispatchor.class);

		Dispatchor concealedDis = (Dispatchor) result;
		Assertions.assertThat(concealedDis.getName()).isEqualTo(mn("dispatched"));
		Assertions.assertThat(concealedDis.getUnderlying()).isEqualTo(mn("revenue"));
		// structural keys preserved
		Assertions.assertThat(concealedDis.getDecompositionKey()).isEqualTo("percentBucket");
		Assertions.assertThat(concealedDis.getAggregationKey()).isNotEmpty();
		// options cleared
		Assertions.assertThat(concealedDis.getDecompositionOptions()).isEmpty();
		Assertions.assertThat(concealedDis.getAggregationOptions()).isEmpty();
	}
}
