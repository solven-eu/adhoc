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
package eu.solven.adhoc.engine.step;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.filter.FilterHelpers;
import eu.solven.adhoc.filter.ISliceFilter;
import eu.solven.adhoc.measure.model.IHasTags;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.options.IQueryOption;
import eu.solven.adhoc.options.StandardQueryOptions;
import eu.solven.adhoc.query.cube.IGroupBy;
import eu.solven.adhoc.util.AdhocUnsafe;
import lombok.Getter;

/**
 * Abstract base for all query steps processed by the cube engine.
 *
 * <p>
 * Holds the common fields ({@code filter}, {@code groupBy}, {@code customMarker}, {@code options}, {@code cache}) and
 * defines the {@code equals}/{@code hashCode} contract via the abstract {@link #getMeasure()} method. Subclasses
 * specialise only the measure field:
 * <ul>
 * <li>{@link CubeQueryStep} — {@link IMeasure} (any measure type)</li>
 * <li>{@code TableQueryStep} — {@link eu.solven.adhoc.measure.model.Aggregator} (strongly typed)</li>
 * </ul>
 *
 * <p>
 * Because {@code equals} delegates to the abstract {@code getMeasure()}, a {@link CubeQueryStep} and a
 * {@code TableQueryStep} carrying the same measure, filter, groupBy, customMarker, and options are considered equal.
 * This lets {@code TableQueryStep} values be looked up in maps keyed by {@link CubeQueryStep} without any unwrapping.
 *
 * @author Benoit Lacelle
 * @see CubeQueryStep
 * @see ICubeQueryStep
 */
@Getter
public abstract class ACubeQueryStep implements ICubeQueryStep {
	public static final String KEY_CACHE_TRANSVERSE = "adhoc-transverseCache";
	public static final String KEY_FILTER_OPTIMIZER = "adhoc-filterOptimizer";

	private final long id = AdhocUnsafe.nextQueryStepIndex();

	private final ISliceFilter filter;
	private final IGroupBy groupBy;
	private final Object customMarker;
	private final ImmutableSet<IQueryOption> options;
	// Should be a threadSafe implementation
	private final Map<Object, Object> cache;

	@SuppressWarnings("checkstyle:AvoidInlineConditionals")
	protected ACubeQueryStep(ISliceFilter filter,
			IGroupBy groupBy,
			Object customMarker,
			ImmutableSet<IQueryOption> options,
			Map<Object, Object> cache) {
		this.filter = filter != null ? filter : ISliceFilter.MATCH_ALL;
		this.groupBy = groupBy != null ? groupBy : IGroupBy.GRAND_TOTAL;
		// Unwrap Optional so the stored value is never Optional
		this.customMarker = customMarker instanceof Optional<?> opt ? opt.orElse(null) : customMarker;
		this.options = options != null ? options : ImmutableSet.of();
		this.cache = cache != null ? cache : new ConcurrentHashMap<>();
	}

	@Override
	public abstract IMeasure getMeasure();

	@Override
	public boolean isDebug() {
		return getOptions().contains(StandardQueryOptions.DEBUG) || getMeasure().getTags().contains(IHasTags.TAG_DEBUG);
	}

	public void setCrossStepsCache(Map<Object, Object> transverseCache) {
		getCache().put(KEY_CACHE_TRANSVERSE, transverseCache);
	}

	@Override
	public void invalidateAll() {
		Map<Object, Object> transverseCache = getTransverseCache();
		cache.clear();
		setCrossStepsCache(transverseCache);
	}

	public Map<Object, Object> getTransverseCache() {
		Map<Object, Object> transverseCache = (Map<Object, Object>) getCache().get(KEY_CACHE_TRANSVERSE);

		if (transverseCache == null) {
			throw new IllegalStateException("Missing call to `setCrossStepsCache` on %s".formatted(this));
		}
		return transverseCache;
	}

	/**
	 * Returns all columns referenced by the given step (groupBy columns + filter columns).
	 */
	public static Set<String> getColumns(ACubeQueryStep step) {
		return ImmutableSet.<String>builder()
				.addAll(step.getGroupBy().getGroupedByColumns())
				.addAll(FilterHelpers.getFilteredColumns(step.getFilter()))
				.build();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof ACubeQueryStep other)) {
			return false;
		}
		return Objects.equals(getMeasure(), other.getMeasure()) && Objects.equals(filter, other.filter)
				&& Objects.equals(groupBy, other.groupBy)
				&& Objects.equals(customMarker, other.customMarker)
				&& Objects.equals(options, other.options);
	}

	@Override
	public int hashCode() {
		return Objects.hash(getMeasure(), filter, groupBy, customMarker, options);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "(id="
				+ id
				+ ", measure="
				+ getMeasure()
				+ ", filter="
				+ filter
				+ ", groupBy="
				+ groupBy
				+ ", customMarker="
				+ customMarker
				+ ", options="
				+ options
				+ ")";
	}
}
