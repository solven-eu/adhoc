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
package eu.solven.adhoc.cube;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.NonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.column.IColumnsManager;
import eu.solven.adhoc.column.generated_column.ColumnGeneratorHelpers;
import eu.solven.adhoc.column.generated_column.IColumnGenerator;
import eu.solven.adhoc.engine.ICubeQueryEngine;
import eu.solven.adhoc.filter.value.IValueMatcher;
import eu.solven.adhoc.measure.forest.IMeasureForest;
import eu.solven.adhoc.measure.operator.IHasOperatorFactory;
import eu.solven.adhoc.measure.operator.IOperatorFactory;
import eu.solven.adhoc.model.measure.IAdhocTags;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.table.transcoder.AliasingContext;
import eu.solven.adhoc.util.AdhocUnsafe;
import eu.solven.adhoc.util.IHasCache;
import eu.solven.adhoc.util.map.AdhocMapPathGet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Owns the cube's column-resolution subsystem on behalf of {@link CubeWrapper}: collects table columns, calculated
 * columns from the {@link IColumnsManager}, generated columns produced by measures, then attaches caller-facing aliases
 * and duplicates each metadata entry under its alias name.
 *
 * <p>
 * Instantiated once per {@link CubeWrapper}; carries genuine state (the memoised {@code getColumns()} result) and is
 * therefore a plain class rather than a static helper. The cache is a single-value lazy memo: the first call computes
 * and stores; subsequent calls return the same {@link Collection}. {@link #invalidateAll()} swaps in a fresh memo so
 * the next {@link #getColumns()} re-computes.
 *
 * <p>
 * Caching this result also dedupes the {@link org.slf4j.Logger#warn} entries about discarded aliases (one warning at
 * first computation rather than once per call site), which is the second-order benefit besides the performance win on
 * large cubes.
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class CubeColumnsWrapper implements IHasCache {

	@NonNull
	private final ITableWrapper table;

	@NonNull
	private final IColumnsManager columnsManager;

	@NonNull
	private final IMeasureForest forest;

	@NonNull
	private final ICubeQueryEngine engine;

	/** Log-context tag — written into the warning when discarding an alias with no underlying column. */
	@NonNull
	private final String cubeName;

	// Single-key Guava LoadingCache: thread-safe lazy memo with explicit invalidateAll() support. The cache is loaded
	// once (key=Boolean.TRUE) and reused across calls; invalidation drops the entry so the next getColumns()
	// recomputes.
	private final LoadingCache<Boolean, Collection<ColumnMetadata>> cached =
			CacheBuilder.newBuilder().build(CacheLoader.from(this::computeColumns));

	/**
	 * @return the cube's column metadata: table columns + calculated columns + generated columns, each duplicated under
	 *         every caller-facing alias declared on the {@link IColumnsManager}. Cached; subsequent calls return the
	 *         same {@link Collection} until {@link #invalidateAll()} is invoked.
	 */
	public Collection<ColumnMetadata> getColumns() {
		return cached.getUnchecked(Boolean.TRUE);
	}

	/** Drops the cached metadata so the next {@link #getColumns()} re-computes. */
	@Override
	public void invalidateAll() {
		cached.invalidateAll();
	}

	protected Collection<ColumnMetadata> computeColumns() {
		Map<String, ColumnMetadata> columnToType = getColumnsWithoutAliases();

		AliasingContext transcodingContext = columnsManager.openTranscodingContext();

		// Register aliases in the `alias` field of metadata
		// TODO This does not handle recursive aliases
		columnsManager.getColumnAliases().forEach(columnAlias -> {
			String tableName = transcodingContext.underlying(columnAlias);

			ColumnMetadata originalMetadata = columnToType.get(tableName);

			if (originalMetadata == null) {
				// Typically happens on JOINs
				// SQL engines generally returns unqualified columnName
				// Hence, `joinA.columnC` is returned as `columnC` by SQL.
				// But `columnC` is an alias for `joinA.columnC` according to the aliaser.
				// In most situations, we prefer to fallback on the alias, as the project alias is often the same as the
				// SQL alias (i.e. unqualified columnName).
				originalMetadata = columnToType.get(columnAlias);
			}

			if (originalMetadata == null && tableName != null) {
				// Third-try: a JooqTableSupplierBuilder-style aliaser declares `aliasedColor -> b.color`, but the
				// table only knows the unqualified `color` (see JooqTableWrapper.getColumns: SQL backends return
				// column names without their table-qualifier when listing columns). Strip the qualifier and the
				// surrounding jOOQ quotes (e.g. `"b"."color"` → `color`) so the bare lookup matches the table's
				// column names. CubeColumnsWrapper sits above the SQL layer and does not own a {@code Parser},
				// hence this string-level handling rather than the dialect-aware
				// {@code AdhocJooqHelper.unqualifiedColumnName(...)}; consequence: column names that contain
				// unquoted dots are NOT supported here. Such cases need a parser-aware path (likely surfacing
				// the original as a structured {@code Name} from the supplier rather than a String).
				String unqualified = stripQualifierAndQuotes(tableName);
				if (!unqualified.equals(tableName)) {
					originalMetadata = columnToType.get(unqualified);
				}
			}

			if (originalMetadata == null) {
				String minimizing = AdhocMapPathGet.minimizingDistance(columnToType.keySet(), tableName);

				// Discard: a shared ColumnsManager may carry aliases relevant only to a subset of cubes, so an alias
				// with no underlying column on this cube is not necessarily a bug — but still worth warning about.
				log.warn("Discarding alias={} (tableName={}) as no underlying column on cube={}. Similar with c={}",
						columnAlias,
						tableName,
						cubeName,
						minimizing);
			} else {
				columnToType.put(originalMetadata.getName(), originalMetadata.toBuilder().alias(columnAlias).build());
			}
		});

		// Duplicate each column given its alias
		Map<String, ColumnMetadata> aliasToColumn = new LinkedHashMap<>();
		columnToType.forEach((column, metadata) -> {
			metadata.getAliases().forEach(alias -> {
				aliasToColumn.put(alias, metadata.toBuilder().name(alias).alias(column).build());
			});
		});

		columnToType.putAll(aliasToColumn);

		return columnToType.values();
	}

	protected Map<String, ColumnMetadata> getColumnsWithoutAliases() {
		Map<String, ColumnMetadata> columnToType = new LinkedHashMap<>();

		// First, register table columns
		table.getColumns().forEach((tableColumn) -> {
			columnToType.put(tableColumn.getName(), tableColumn);
		});

		// Then, register calculated columns (e.g. based on an expression)
		columnsManager.getColumnTypes().forEach((columnName, type) -> {
			columnToType.put(columnName,
					ColumnMetadata.builder().name(columnName).tag(IAdhocTags.TAG_CALCULATED).type(type).build());
		});

		IOperatorFactory operatorFactory = IHasOperatorFactory.getOperatorsFactory(engine);
		forest.getMeasures().forEach(measure -> {
			try {
				ColumnGeneratorHelpers
						.getColumnGenerators(operatorFactory, ImmutableSet.of(measure), IValueMatcher.MATCH_ALL)
						.forEach(columnGenerator -> {
							// TODO How conflicts should be handled? `ColumnMetadata.merge`?
							columnGenerator.getColumnTypes().forEach((columnName, type) -> {
								columnToType.put(columnName,
										ColumnMetadata.builder()
												.name(columnName)
												.tag(IAdhocTags.TAG_GENERATED)
												.type(type)
												.build());
							});
						});
			} catch (RuntimeException e) {
				if (AdhocUnsafe.isFailFast()) {
					String msg = "Issue looking for an %s in m=%s c=%s"
							.formatted(IColumnGenerator.class.getSimpleName(), measure, cubeName);
					throw new IllegalStateException(msg, e);
				} else {
					log.warn("Issue looking for an {} in m={} c={}",
							IColumnGenerator.class.getSimpleName(),
							measure,
							cubeName,
							e);
				}
			}
		});
		return columnToType;
	}

	/**
	 * String-level qualifier stripper. Removes the segment before the last unquoted dot, then unwraps surrounding
	 * double-quotes — covers the two shapes the supplier can hand us: bare-dotted ({@code b.color}) and jOOQ-escaped
	 * two-part ({@code "b"."color with space"}). Does NOT support column names containing unquoted dots, since this
	 * layer has no parser; for that, plumb a dialect-aware path instead.
	 */
	protected static String stripQualifierAndQuotes(String qualifiedName) {
		int lastDot = qualifiedName.lastIndexOf('.');
		String unqualified;
		if (lastDot >= 0 && lastDot < qualifiedName.length() - 1) {
			unqualified = qualifiedName.substring(lastDot + 1);
		} else {
			unqualified = qualifiedName;
		}
		if (unqualified.length() >= 2 && unqualified.charAt(0) == '"'
				&& unqualified.charAt(unqualified.length() - 1) == '"') {
			unqualified = unqualified.substring(1, unqualified.length() - 1);
		}
		return unqualified;
	}
}
