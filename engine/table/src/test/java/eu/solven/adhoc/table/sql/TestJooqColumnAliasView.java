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
package eu.solven.adhoc.table.sql;

import java.util.Map;

import org.assertj.core.api.Assertions;
import org.jooq.SQLDialect;
import org.jooq.TableLike;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import eu.solven.adhoc.query.table.TableQueryV4;

public class TestJooqColumnAliasView {

	/** Helper: minimal supplier with no aliases and no column-to-join mapping. */
	private IJooqTableSupplier emptySupplier() {
		return IJooqTableSupplier.constant(DSL.table("t"));
	}

	/** Helper: supplier that returns the provided alias and column-owner maps. */
	private IJooqTableSupplier supplierWithMaps(Map<String, String> aliasToOriginal, Map<String, String> columnToJoin) {
		return new IJooqTableSupplier() {
			@Override
			public TableLike<?> tableFor(TableQueryV4 tableQuery) {
				return DSL.table("t");
			}

			@Override
			public TableLike<?> getSchemaTable() {
				return DSL.table("t");
			}

			@Override
			public Map<String, String> getAliasToOriginal() {
				return aliasToOriginal;
			}

			@Override
			public Map<String, String> getColumnToJoinAlias() {
				return columnToJoin;
			}
		};
	}

	// ---- fieldOwner ----

	@Test
	public void testFieldOwner_emptySupplier_returnsNull() {
		JooqColumnAliasView view =
				JooqColumnAliasView.from(emptySupplier(), () -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.fieldOwner("col1")).isNull();
		Assertions.assertThat(view.fieldOwner("anything")).isNull();
	}

	@Test
	public void testFieldOwner_knownColumn_returnsOwner() {
		Map<String, String> columnToJoin = Map.of("col1", "join1", "col2", "join2");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(Map.of(), columnToJoin),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.fieldOwner("col1")).isEqualTo("join1");
		Assertions.assertThat(view.fieldOwner("col2")).isEqualTo("join2");
		Assertions.assertThat(view.fieldOwner("unknown")).isNull();
	}

	// ---- effectiveName (no aliases) ----

	@Test
	public void testEffectiveName_noAliases_alwaysReturnsBare() {
		Map<String, String> columnToJoin = Map.of("col1", "join1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(Map.of(), columnToJoin),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.effectiveName("col1", "join1")).isEqualTo("col1");
		Assertions.assertThat(view.effectiveName("col1", null)).isEqualTo("col1");
	}

	// ---- effectiveName (with aliases) ----

	@Test
	public void testEffectiveName_qualifiedAlias_differentOwner_shadowingApplies() {
		// "alias1" is declared as pointing to "join2.col1".
		// A field named "alias1" owned by "join1" gets renamed to "join1.alias1"
		// because the alias claims the bare name "alias1" for a different join.
		Map<String, String> aliasToOriginal = Map.of("alias1", "join2.col1");
		Map<String, String> columnToJoin = Map.of("alias1", "join1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, columnToJoin),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		String effective = view.effectiveName("alias1", "join1");
		Assertions.assertThat(effective).isEqualTo(AdhocJooqHelper.qualifiedColumnName("join1", "alias1"));
	}

	@Test
	public void testEffectiveName_qualifiedAlias_sameOwner_noShadowing() {
		// "alias1" → "join1.col1": target owner equals field owner → no shadowing
		Map<String, String> aliasToOriginal = Map.of("alias1", "join1.col1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.effectiveName("alias1", "join1")).isEqualTo("alias1");
	}

	@Test
	public void testEffectiveName_nullFieldOwner_noShadowing() {
		// When fieldOwner is null the condition short-circuits and returns bare name
		Map<String, String> aliasToOriginal = Map.of("alias1", "join2.col1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.effectiveName("alias1", null)).isEqualTo("alias1");
	}

	@Test
	public void testEffectiveName_aliasNameEqualsTargetColumn_noShadowing() {
		// fieldName == targetColumn: natural ON-clause registration, not a real shadow
		// "col1" → "join2.col1": targetColumn="col1", fieldName="col1" → equal → no shadowing
		Map<String, String> aliasToOriginal = Map.of("col1", "join2.col1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.effectiveName("col1", "join1")).isEqualTo("col1");
	}

	// ---- aliasesForTarget ----

	@Test
	public void testAliasesForTarget_noAliases_alwaysEmpty() {
		JooqColumnAliasView view =
				JooqColumnAliasView.from(emptySupplier(), () -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.aliasesForTarget("col1", "join1")).isEmpty();
		Assertions.assertThat(view.aliasesForTarget("col1", null)).isEmpty();
	}

	@Test
	public void testAliasesForTarget_unqualifiedAlias_attachesToAnyOwner() {
		// "myAlias" → unqualified "col1": no targetOwner → attaches regardless of fieldOwner
		Map<String, String> aliasToOriginal = Map.of("myAlias", "col1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.aliasesForTarget("col1", "join1")).containsExactly("myAlias");
		Assertions.assertThat(view.aliasesForTarget("col1", null)).containsExactly("myAlias");
		Assertions.assertThat(view.aliasesForTarget("col2", "join1")).isEmpty();
	}

	@Test
	public void testAliasesForTarget_qualifiedAlias_matchesOwnerOnly() {
		// "myAlias" → "join1.col1": only attaches when fieldOwner == "join1"
		Map<String, String> aliasToOriginal = Map.of("myAlias", "join1.col1");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.aliasesForTarget("col1", "join1")).containsExactly("myAlias");
		Assertions.assertThat(view.aliasesForTarget("col1", "join2")).isEmpty();
		Assertions.assertThat(view.aliasesForTarget("col1", null)).isEmpty();
		Assertions.assertThat(view.aliasesForTarget("other", "join1")).isEmpty();
	}

	@Test
	public void testAliasesForTarget_multipleAliases_onlyMatchingReturned() {
		// Two aliases pointing to different columns
		Map<String, String> aliasToOriginal = Map.of("aliasA", "col1", "aliasB", "col2");
		JooqColumnAliasView view = JooqColumnAliasView.from(supplierWithMaps(aliasToOriginal, Map.of()),
				() -> DSL.using(SQLDialect.DUCKDB).parser());

		Assertions.assertThat(view.aliasesForTarget("col1", null)).containsExactly("aliasA");
		Assertions.assertThat(view.aliasesForTarget("col2", null)).containsExactly("aliasB");
	}
}
