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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.jooq.Name;
import org.jooq.Parser;
import org.jspecify.annotations.Nullable;

import com.google.common.collect.ImmutableSet;

/**
 * Snapshot of the alias / column-owner state needed to decorate the column list returned by
 * {@link JooqTableWrapper#getColumns()}: which join owns each bare column name, which caller-facing aliases attach to
 * each field, and the qualified form to use when an alias shadows a same-named real column.
 *
 * <p>
 * Extracted from {@link JooqTableWrapper} to lower coupling: the wrapper only needs three queries
 * ({@link #fieldOwner(String)}, {@link #effectiveName(String, String)}, {@link #aliasesForTarget(String, String)}) and
 * doesn't have to know how the underlying maps are populated. {@link #from(IJooqTableSupplier, Supplier)} is the only
 * factory; everything else is read-only.
 *
 * @author Benoit Lacelle
 */
public final class JooqColumnAliasView {

	private final Map<String, String> bareToOwner;
	private final Map<String, String> aliasNameToTargetOwner;
	private final Map<String, String> aliasNameToTargetColumn;

	private JooqColumnAliasView(Map<String, String> bareToOwner,
			Map<String, String> aliasNameToTargetOwner,
			Map<String, String> aliasNameToTargetColumn) {
		this.bareToOwner = bareToOwner;
		this.aliasNameToTargetOwner = aliasNameToTargetOwner;
		this.aliasNameToTargetColumn = aliasNameToTargetColumn;
	}

	/**
	 * Builds a view from the supplier's alias map and column-owner index. The supplier-side maps are read once and
	 * memoised in the returned view — recompute by calling this factory again after a schema change /
	 * {@code invalidateAll()}.
	 *
	 * @param supplier
	 *            the table's {@link IJooqTableSupplier}; an instance with empty {@code getAliasToOriginal()} yields a
	 *            view that's effectively a no-op (every {@code effectiveName} is the bare field name, every
	 *            {@code aliasesForTarget} is empty)
	 * @param parserSupplier
	 *            dialect-aware parser supplier — same shape as {@link AdhocJooqHelper#name(String, Supplier)}
	 * @return a fresh view
	 */
	public static JooqColumnAliasView from(IJooqTableSupplier supplier, Supplier<Parser> parserSupplier) {
		Map<String, String> aliasToOriginal = supplier.getAliasToOriginal();
		Map<String, String> bareToOwner = supplier.getColumnToJoinAlias();
		if (aliasToOriginal.isEmpty()) {
			return new JooqColumnAliasView(bareToOwner, Map.of(), Map.of());
		}

		Map<String, String> aliasNameToTargetOwner = new LinkedHashMap<>();
		Map<String, String> aliasNameToTargetColumn = new LinkedHashMap<>();
		aliasToOriginal.forEach((alias, original) -> {
			Name name = AdhocJooqHelper.name(original, parserSupplier);
			aliasNameToTargetColumn.put(alias, name.last());
			Name[] parts = name.parts();
			if (parts.length >= 2) {
				aliasNameToTargetOwner.put(alias, parts[0].last());
			}
		});
		return new JooqColumnAliasView(bareToOwner, aliasNameToTargetOwner, aliasNameToTargetColumn);
	}

	/**
	 * @param fieldName
	 *            the bare column name as reported by the SQL backend (no qualifier)
	 * @return the owning join alias if known to the supplier's column→join index; {@code null} otherwise (typically for
	 *         synthetic / expression columns that aren't in the index)
	 */
	public @Nullable String fieldOwner(String fieldName) {
		return bareToOwner.get(fieldName);
	}

	/**
	 * Computes the effective name for a column in the cube's column list. Renames a field to its qualified
	 * {@code <owner>.<name>} form when a USER-DECLARED alias claims its bare name for a column owned by a different
	 * join. Excludes natural ON-clause auto-registrations (alias name equal to target column name) — those are not
	 * shadowing anything; they just record that the bare name canonically resolves to the LEFT/parent table on
	 * natural-key joins.
	 *
	 * @param fieldName
	 *            the bare column name
	 * @param fieldOwner
	 *            the result of {@link #fieldOwner(String)} for this field, or {@code null} if unknown
	 * @return the bare {@code fieldName} when no shadowing applies; otherwise the qualified form via
	 *         {@link AdhocJooqHelper#qualifiedColumnName(String, String)}
	 */
	public String effectiveName(String fieldName, @Nullable String fieldOwner) {
		String shadowingTargetOwner = aliasNameToTargetOwner.get(fieldName);
		String shadowingTargetColumn = aliasNameToTargetColumn.get(fieldName);
		if (shadowingTargetOwner != null && fieldOwner != null
				&& !shadowingTargetOwner.equals(fieldOwner)
				&& !fieldName.equals(shadowingTargetColumn)) {
			return AdhocJooqHelper.qualifiedColumnName(fieldOwner, fieldName);
		}
		return fieldName;
	}

	/**
	 * @param fieldName
	 *            the bare column name
	 * @param fieldOwner
	 *            the result of {@link #fieldOwner(String)} for this field, or {@code null} if unknown
	 * @return the caller-facing alias names that attach to this field — only the aliases whose qualified original
	 *         points exactly at this field (matched on column name AND, when the alias's target was qualified, on
	 *         owning join). When the alias's target was unqualified, the alias attaches to any same-named field.
	 */
	public ImmutableSet<String> aliasesForTarget(String fieldName, @Nullable String fieldOwner) {
		if (aliasNameToTargetColumn.isEmpty()) {
			return ImmutableSet.of();
		}
		ImmutableSet.Builder<String> attached = ImmutableSet.builder();
		aliasNameToTargetColumn.forEach((alias, targetCol) -> {
			if (!targetCol.equals(fieldName)) {
				return;
			}
			String targetOwner = aliasNameToTargetOwner.get(alias);
			// `targetOwner == null` happens when the alias's `original` was unqualified — the alias attaches
			// to any same-named field, since we don't know the intended owner.
			if (targetOwner == null || targetOwner.equals(fieldOwner)) {
				attached.add(alias);
			}
		});
		return attached.build();
	}
}
