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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.exception.DataAccessException;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import eu.solven.adhoc.column.ColumnMetadata;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Owns the cube's column-resolution subsystem on behalf of {@link JooqTableWrapper}: the field-probe cache (TTL-based,
 * async-refreshing), the {@link JooqColumnAliasView} construction, and the decoration pass that turns raw
 * {@link Field}s into a {@link Collection} of {@link ColumnMetadata}.
 *
 * <p>
 * Instantiated once per {@link JooqTableWrapper}; carries genuine state (the cache) and is therefore a plain class
 * rather than a static helper. {@link #invalidateAll()} drops the cached field probe; the wrapper's
 * {@link JooqTableWrapper#invalidateAll()} delegates here.
 *
 * <p>
 * Pipeline (one call to {@link #getColumns()}):
 * <ol>
 * <li>Fetch fields from the cache, re-probing once if the first result is empty (covers the missing-files retry the
 * wrapper used to do inline).</li>
 * <li>Build a {@link JooqColumnAliasView} from the supplier's alias / column-owner state and the parser supplier.</li>
 * <li>For each field: ask the view for its owner / effective name / attached aliases.</li>
 * <li>Resolve the column's Java type via {@code fieldTypeResolver} (the wrapper's type-promotion hook stays out of this
 * class).</li>
 * <li>Keep a {@link LinkedHashMap} keyed by effective name; same-name collisions overwrite, with debug log on each and
 * a warning when the conflicting columns also disagree on type.</li>
 * </ol>
 *
 * @author Benoit Lacelle
 */
@RequiredArgsConstructor
@Slf4j
public class JooqTableColumnsWrapper {

	@NonNull
	private final JooqTableWrapperParameters tableParameters;

	/**
	 * Converts a raw {@link Field} to the Java type to surface in the {@link ColumnMetadata}. Lets the wrapper apply
	 * its own type promotions (e.g. {@code java.sql.Date → java.time.LocalDate}) without leaking that knowledge into
	 * this class.
	 */
	@NonNull
	private final Function<Field<?>, Class<?>> fieldTypeResolver;

	/** Log-context tag — written into the warning when same-named columns disagree on type. */
	@NonNull
	private final String tableName;

	// Lazy / memoised: the cache reads `tableParameters.getFieldsCacheRefreshAfterWrite()`, which is only
	// captured once we hit `.get()`. That fits the same construction pattern the wrapper used to apply when
	// `tableParameters` was assigned by the all-args constructor — and now also future-proofs us against
	// callers that mutate parameters between construction and first lookup.
	private final Supplier<LoadingCache<Object, List<Field<?>>>> fieldsCache = Suppliers.memoize(this::makeFieldsCache);

	/** Drops every cached entry — call after a schema change so the next {@link #getColumns()} re-probes. */
	public void invalidateAll() {
		fieldsCache.get().invalidateAll();
	}

	/**
	 * @return the cube's column metadata: one entry per distinct field name with effective-name renaming and
	 *         caller-facing aliases attached. Empty when the field probe returned no fields.
	 */
	public Collection<ColumnMetadata> getColumns() {
		List<Field<?>> fields = getFields();

		// https://duckdb.org/docs/sql/expressions/star.html
		Map<String, ColumnMetadata> columnToType = new LinkedHashMap<>();

		// TODO Qualify columns with table
		// https://duckdbsnippets.com/snippets/204/label-columns-based-on-source-table
		// SELECT
		// COLUMNS(t1.*) AS 't1_\0',
		// COLUMNS(t2.*) AS 't2_\0'
		// FROM range(10) t1
		// JOIN range(10) t2 ON t1.range = t2.range

		JooqColumnAliasView aliasView = JooqColumnAliasView.from(tableParameters.getTableSupplier(),
				() -> tableParameters.getDslSupplier().getDSLContext().parser());

		fields.forEach(field -> {
			String fieldName = field.getName();
			String fieldOwner = aliasView.fieldOwner(fieldName);
			String effectiveName = aliasView.effectiveName(fieldName, fieldOwner);
			Set<String> attachedAliases = aliasView.aliasesForTarget(fieldName, fieldOwner);

			Class<?> fieldType = fieldTypeResolver.apply(field);
			ColumnMetadata previousColumn = columnToType.put(effectiveName,
					ColumnMetadata.builder().name(effectiveName).type(fieldType).aliases(attachedAliases).build());
			if (previousColumn != null) {
				log.debug("Multiple columns with same name. Typically happens on a JOIN");
				if (!Objects.equals(fieldType, previousColumn.getType())) {
					log.warn("Multiple columns with same name (table={} column={}), and different types: {} != {}",
							tableName,
							effectiveName,
							previousColumn,
							fieldType);
				}
			}
		});

		return columnToType.values();
	}

	/**
	 * Cache-aware field accessor with one retry when the first probe returns empty. The empty case typically means
	 * "underlying files were missing" — re-probing gives the resolver a chance to see updated state without forcing the
	 * caller to call {@link #invalidateAll()} first.
	 */
	protected List<Field<?>> getFields() {
		LoadingCache<Object, List<Field<?>>> cache = fieldsCache.get();
		List<Field<?>> fields = cache.getUnchecked(Boolean.TRUE);

		if (fields.isEmpty()) {
			// Fields is typically empty if we were missing some files: let's retry
			cache.invalidateAll();
			fields = cache.getUnchecked(Boolean.TRUE);
		}

		return fields;
	}

	/** Direct probe — bypasses the cache. Loaded by the cache and exposed for tests / debugging. */
	protected List<Field<?>> noCacheGetFields() {
		// Single source of truth: the parameters' columnsResolver (guaranteed non-null via the custom getter, which
		// defaults to `JooqColumnsHelpers.dbProbe(dslSupplier)` when the builder did not configure one).
		try {
			List<Field<?>> fields = tableParameters.getColumnsResolver()
					.columnsOf(tableParameters.getDslSupplier(), tableParameters.getTableSupplier().getSchemaTable());
			if (fields == null) {
				return Collections.emptyList();
			} else {
				return List.copyOf(fields);
			}
		} catch (DataAccessException e) {
			if (Objects.requireNonNullElse(e.getMessage(), "")
					.contains("IO Error: No files found that match the pattern")) {
				if (log.isDebugEnabled()) {
					log.warn("No column for table=`{}` due to missing files", tableName, e);
				} else {
					// The failure may be missing anywhere in the SQL (e.g. the main `FROM`, or any `JOIN`)
					log.warn("No column for table=`{}` due to missing files. sqlMsg={}", tableName, e.getMessage());
				}
				return Collections.emptyList();
			} else {
				throw e;
			}
		}
	}

	private LoadingCache<Object, List<Field<?>>> makeFieldsCache() {
		return CacheBuilder.newBuilder()
				// https://github.com/google/guava/wiki/cachesexplained#refresh
				.refreshAfterWrite(tableParameters.getFieldsCacheRefreshAfterWrite())
				.removalListener(new RemovalListener<Object, List<Field<?>>>() {

					@Override
					public void onRemoval(RemovalNotification<Object, List<Field<?>>> notification) {
						RemovalCause cause = notification.getCause();
						List<Field<?>> removedFields = notification.getValue();
						log.debug("Removing fields for {} due to {} (were {})", tableName, cause, removedFields);
					}
				})
				.build(CacheLoader.asyncReloading(CacheLoader.from(this::noCacheGetFields),
						AdhocUnsafe.maintenancePool));
	}
}
