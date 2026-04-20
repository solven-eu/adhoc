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

	final MeasureForestConcealer concealer = MeasureForestConcealer.builder().build();

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

	/** Returns the concealed operator key (k_ prefix). */
	private static String kn(String originalKey) {
		return "k_" + String.format("%08x", originalKey.hashCode() & 0xFFFFFFFFL);
	}

	/** Returns the concealed value token (v_ prefix). */
	private static String vn(Object originalValue) {
		return "v_" + String.format("%08x", java.util.Objects.hashCode(originalValue) & 0xFFFFFFFFL);
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
		// custom aggregationKey is concealed (not in the standard-keys whitelist)
		Assertions.assertThat(concealedAgg.getAggregationKey()).isEqualTo(kn("customAggKey"));
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
		// custom combinationKey is concealed; options cleared
		Assertions.assertThat(concealedComb.getCombinationKey()).isEqualTo(kn("customDivideKey"));
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
		// column name AND operand value are concealed in the filter
		Assertions.assertThat(concealedFil.getFilter()).isEqualTo(ColumnFilter.matchEq(cn("ccy"), vn("EUR")));
	}

	// Filtrator with a composite filter: AND(col1=v1, OR(col2=v2), NOT(col3=v3)).
	// All column names must be hashed; values and logical structure are preserved.
	@Test
	public void testFiltrator_compositeFilter_allColumnsConcealed() {
		ISliceFilter compositeFilter = AndFilter.and(ColumnFilter.matchEq("col1", "v1"),
				OrFilter.or("col2", "v2"),
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
		// custom editorKey is concealed; editorOptions cleared
		Assertions.assertThat(concealedShiftor.getEditorKey()).isEqualTo(kn("someEditorKey"));
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
		// custom combinationKey is concealed
		Assertions.assertThat(concealedParent.getCombinationKey()).isEqualTo(kn("secretFormula"));
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

		Map<String, String> mapping = MeasureForestConcealer.builder().build().buildMapping("m_", names);

		Assertions.assertThat(mapping.get("Aa")).isEqualTo("m_00000840");
		Assertions.assertThat(mapping.get("BB")).isEqualTo("m_00000840_2");
	}

	@Test
	public void testHashLength_4digits() {
		// hashCode of "hello" = 99162322 = 0x05e918d2; truncated to 4 hex digits → 0x18d2
		MeasureForestConcealer short4 = MeasureForestConcealer.builder().hashLength(4).build();
		Map<String, String> mapping = short4.buildMapping("m_", Set.of("hello"));
		Assertions.assertThat(mapping.get("hello")).isEqualTo("m_18d2");
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
		Assertions.assertThat(concealedPar.getGroupBy().getSortedColumns())
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
		// custom decompositionKey is concealed; default aggregationKey (SUM) is in the standard whitelist
		Assertions.assertThat(concealedDis.getDecompositionKey()).isEqualTo(kn("percentBucket"));
		Assertions.assertThat(concealedDis.getAggregationKey()).isEqualTo("SUM");
		// options cleared
		Assertions.assertThat(concealedDis.getDecompositionOptions()).isEmpty();
		Assertions.assertThat(concealedDis.getAggregationOptions()).isEmpty();
	}

	// ── Operator-key whitelist ───────────────────────────────────────────────

	// Standard keys (SUM, COUNT, identity, …) are left verbatim because they refer to public operators with no
	// secret payload. Only custom keys (e.g. fully-qualified class names) are hashed.
	@Test
	public void testAggregator_standardKeyPreserved() {
		Aggregator secret = Aggregator.builder().name("revenue").columnName("col").aggregationKey("SUM").build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(secret).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Aggregator concealedAgg = (Aggregator) concealed.getNameToMeasure().get(mn("revenue"));
		Assertions.assertThat(concealedAgg.getAggregationKey()).isEqualTo("SUM");
	}

	// A user-supplied per-kind whitelist fully replaces the default, enabling integration with custom operator
	// factories. Here we override only the aggregation whitelist.
	@Test
	public void testCustomStandardKeys_overrideDefault() {
		MeasureForestConcealer custom = MeasureForestConcealer.builder()
				.standardAggregationKeys(java.util.Set.of("myCustomSum"))
				.build();
		Aggregator withCustom = Aggregator.builder().name("m1").columnName("c").aggregationKey("myCustomSum").build();
		Aggregator withSum = Aggregator.builder().name("m2").columnName("c2").aggregationKey("SUM").build();
		IMeasureForest forest = MeasureForest.builder().name("myForest").measure(withCustom).measure(withSum).build();

		IMeasureForest concealed = custom.conceal(forest);

		Aggregator mappedCustom = (Aggregator) concealed.getNameToMeasure().get(mn("m1"));
		Aggregator mappedSum = (Aggregator) concealed.getNameToMeasure().get(mn("m2"));
		// "myCustomSum" is now in the aggregation whitelist → kept verbatim
		Assertions.assertThat(mappedCustom.getAggregationKey()).isEqualTo("myCustomSum");
		// "SUM" is no longer in the aggregation whitelist → hashed
		Assertions.assertThat(mappedSum.getAggregationKey()).isEqualTo(kn("SUM"));
	}

	// The same key string ("SUM") is a standard aggregation AND a standard combination key. The per-kind whitelist
	// makes sure the context determines which branch is applied: concealing an aggregator-SUM still leaves a
	// combinator-SUM untouched, and vice-versa.
	@Test
	public void testStandardKeys_perKindIndependence() {
		// Keep "SUM" standard for aggregations only.
		MeasureForestConcealer aggOnly = MeasureForestConcealer.builder()
				.standardCombinationKeys(java.util.Set.of())
				.build();
		Aggregator agg = Aggregator.builder().name("a").columnName("c").aggregationKey("SUM").build();
		Combinator comb = Combinator.builder().name("c1").underlying("a").combinationKey("SUM").build();
		IMeasureForest forest = MeasureForest.builder().name("f").measure(agg).measure(comb).build();

		IMeasureForest concealed = aggOnly.conceal(forest);

		Aggregator mappedAgg = (Aggregator) concealed.getNameToMeasure().get(mn("a"));
		Combinator mappedComb = (Combinator) concealed.getNameToMeasure().get(mn("c1"));
		Assertions.assertThat(mappedAgg.getAggregationKey()).isEqualTo("SUM");
		Assertions.assertThat(mappedComb.getCombinationKey()).isEqualTo(kn("SUM"));
	}

	// ── IValueMatcher operand concealment ───────────────────────────────────

	// EqualsMatcher operand is concealed via the v_ prefix.
	@Test
	public void testFiltrator_equalsOperandConcealed() {
		Filtrator fil = Filtrator.builder()
				.name("usdRevenue")
				.underlying("revenue")
				.filter(ColumnFilter.matchEq("ccy", "USD"))
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(fil).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Filtrator concealedFil = (Filtrator) concealed.getNameToMeasure().get(mn("usdRevenue"));
		Assertions.assertThat(concealedFil.getFilter()).isEqualTo(ColumnFilter.matchEq(cn("ccy"), vn("USD")));
	}

	// InMatcher operands are concealed one by one.
	@Test
	public void testFiltrator_inOperandsConcealed() {
		Filtrator fil = Filtrator.builder()
				.name("g7Revenue")
				.underlying("revenue")
				.filter(ColumnFilter.builder().column("country").matchIn(java.util.List.of("FR", "DE", "US")).build())
				.build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(fil).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Filtrator concealedFil = (Filtrator) concealed.getNameToMeasure().get(mn("g7Revenue"));
		ColumnFilter concealedCF = (ColumnFilter) concealedFil.getFilter();
		Assertions.assertThat(concealedCF.getColumn()).isEqualTo(cn("country"));
		Assertions.assertThat(concealedCF.getValueMatcher()).isInstanceOf(eu.solven.adhoc.filter.value.InMatcher.class);
		eu.solven.adhoc.filter.value.InMatcher im =
				(eu.solven.adhoc.filter.value.InMatcher) concealedCF.getValueMatcher();
		Assertions.assertThat(new LinkedHashSet<Object>(im.getOperands()))
				.containsExactlyInAnyOrder(vn("FR"), vn("DE"), vn("US"));
	}

	// NotMatcher recursively conceals its negated matcher's operand.
	@Test
	public void testFiltrator_notWrappedOperandConcealed() {
		ISliceFilter notEurope = ColumnFilter.builder()
				.column("country")
				.valueMatcher(eu.solven.adhoc.filter.value.NotMatcher.notEqualTo("FR"))
				.build();
		Filtrator fil = Filtrator.builder().name("nonFr").underlying("revenue").filter(notEurope).build();
		IMeasureForest forest =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(fil).build();

		IMeasureForest concealed = concealer.conceal(forest);

		Filtrator concealedFil = (Filtrator) concealed.getNameToMeasure().get(mn("nonFr"));
		ColumnFilter concealedCF = (ColumnFilter) concealedFil.getFilter();
		Assertions.assertThat(concealedCF.getValueMatcher())
				.isInstanceOf(eu.solven.adhoc.filter.value.NotMatcher.class);
		eu.solven.adhoc.filter.value.NotMatcher nm =
				(eu.solven.adhoc.filter.value.NotMatcher) concealedCF.getValueMatcher();
		// Inner EqualsMatcher now carries a v_-token operand rather than the literal "FR".
		Assertions.assertThat(nm.getNegated()).isEqualTo(eu.solven.adhoc.filter.value.EqualsMatcher.matchEq(vn("FR")));
	}

	// Roundtrip with an InMatcher: conceal then restore recovers the original operand set with correct types.
	@Test
	public void testRestoration_inMatcherRoundtrip() {
		Filtrator fil = Filtrator.builder()
				.name("g7Revenue")
				.underlying("revenue")
				.filter(ColumnFilter.builder().column("country").matchIn(java.util.List.of("FR", "DE", "US")).build())
				.build();
		IMeasureForest original =
				MeasureForest.builder().name("myForest").measure(Aggregator.sum("revenue")).measure(fil).build();

		ConcealingResult result = concealer.concealWithDefinition(original);
		IMeasureForest restored = concealer.restore(result.getConcealedForest(), result.getDefinition());

		Filtrator restoredFil = (Filtrator) restored.getNameToMeasure().get("g7Revenue");
		ColumnFilter restoredCF = (ColumnFilter) restoredFil.getFilter();
		eu.solven.adhoc.filter.value.InMatcher im =
				(eu.solven.adhoc.filter.value.InMatcher) restoredCF.getValueMatcher();
		Assertions.assertThat(new LinkedHashSet<Object>(im.getOperands())).containsExactlyInAnyOrder("FR", "DE", "US");
	}

	// ── Pact with StandardOperatorFactory ────────────────────────────────────
	//
	// Every key exposed by a DEFAULT_STANDARD_*_KEYS constant MUST be instantiable by StandardOperatorFactory's
	// matching factory method (makeAggregation / makeCombination / makeDecomposition / makeEditor). If a key lands
	// in the default `Class.forName` branch, the factory will throw IllegalArgumentException("Unexpected value:")
	// — which is how we catch drift between the whitelist and the factory's actual switch cases.
	//
	// Some keys require a minimal option map to construct (e.g. RANK → "rank" int, SimpleFilterEditor → "shifted").
	// These are provided per-key below; everything else is called with Map.of().

	private static final Map<String, Map<String, ?>> AGG_OPTIONS_PER_KEY = Map.of("RANK", Map.of("rank", 1));

	private static final Map<String, Map<String, ?>> EDITOR_OPTIONS_PER_KEY = Map.of("simple", Map.of("shifted", Map.of()));

	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.MethodSource("defaultStandardAggregationKeys")
	public void testDefaultStandardAggregationKey_instantiable(String key) {
		eu.solven.adhoc.measure.operator.StandardOperatorFactory factory =
				eu.solven.adhoc.measure.operator.StandardOperatorFactory.builder().build();
		Map<String, ?> options = AGG_OPTIONS_PER_KEY.getOrDefault(key, Map.of());
		Assertions.assertThat(factory.makeAggregation(key, options)).as("aggregation key=%s", key).isNotNull();
	}

	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.MethodSource("defaultStandardCombinationKeys")
	public void testDefaultStandardCombinationKey_instantiable(String key) {
		eu.solven.adhoc.measure.operator.StandardOperatorFactory factory =
				eu.solven.adhoc.measure.operator.StandardOperatorFactory.builder().build();
		Assertions.assertThat(factory.makeCombination(key, Map.of())).as("combination key=%s", key).isNotNull();
	}

	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.MethodSource("defaultStandardDecompositionKeys")
	public void testDefaultStandardDecompositionKey_instantiable(String key) {
		eu.solven.adhoc.measure.operator.StandardOperatorFactory factory =
				eu.solven.adhoc.measure.operator.StandardOperatorFactory.builder().build();
		Assertions.assertThat(factory.makeDecomposition(key, Map.of())).as("decomposition key=%s", key).isNotNull();
	}

	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.MethodSource("defaultStandardEditorKeys")
	public void testDefaultStandardEditorKey_instantiable(String key) {
		eu.solven.adhoc.measure.operator.StandardOperatorFactory factory =
				eu.solven.adhoc.measure.operator.StandardOperatorFactory.builder().build();
		Map<String, ?> options = EDITOR_OPTIONS_PER_KEY.getOrDefault(key, Map.of());
		Assertions.assertThat(factory.makeEditor(key, options)).as("editor key=%s", key).isNotNull();
	}

	static java.util.stream.Stream<String> defaultStandardAggregationKeys() {
		return MeasureForestConcealer.DEFAULT_STANDARD_AGGREGATION_KEYS.stream();
	}

	static java.util.stream.Stream<String> defaultStandardCombinationKeys() {
		return MeasureForestConcealer.DEFAULT_STANDARD_COMBINATION_KEYS.stream();
	}

	static java.util.stream.Stream<String> defaultStandardDecompositionKeys() {
		return MeasureForestConcealer.DEFAULT_STANDARD_DECOMPOSITION_KEYS.stream();
	}

	static java.util.stream.Stream<String> defaultStandardEditorKeys() {
		return MeasureForestConcealer.DEFAULT_STANDARD_EDITOR_KEYS.stream();
	}
}
