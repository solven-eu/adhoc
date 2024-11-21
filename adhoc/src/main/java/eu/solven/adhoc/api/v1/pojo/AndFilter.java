/**
 * The MIT License
 * Copyright (c) 2024 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.api.v1.pojo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocFilter;
import eu.solven.adhoc.api.v1.filters.IAndFilter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

/**
 * Default implementation for {@link IAndFilter}
 * 
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@Builder
public class AndFilter implements IAndFilter {

	@Singular
	final List<IAdhocFilter> filters;

	public static AndFilter andAxisEqualsFilters(Map<String, ?> filters) {
		return new AndFilter(filters.entrySet()
				.stream()
				.map(e -> ColumnFilter.builder().column(e.getKey()).matching(e.getValue()).build())
				.collect(Collectors.toList()));
	}

	@Override
	public boolean isNot() {
		return false;
	}

	@Override
	public boolean isMatchAll() {
		// An empty AND is considered to match everything
		boolean empty = filters.isEmpty();

		if (empty) {
			return true;
		}

		if (filters.size() == 1 && filters.get(0).isMatchAll()) {
			return true;
		}

		return false;
	}

	@Override
	public boolean isAnd() {
		return true;
	}

	@Override
	public List<IAdhocFilter> getAnd() {
		return Collections.unmodifiableList(filters);
	}

	@Override
	public String toString() {
		if (isMatchAll()) {
			return "matchAll";
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", filters.size());

		AtomicInteger index = new AtomicInteger();
		filters.stream().limit(5).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocFilter and(IAdhocFilter filter, IAdhocFilter... moreFilters) {
		return and(Lists.asList(filter, moreFilters));
	}

	public static IAdhocFilter and(List<? extends IAdhocFilter> filters) {
		// Skipping matchAll is useful on `.edit`
		List<? extends IAdhocFilter> notMatchAll =
				filters.stream().filter(f -> !f.isMatchAll()).collect(Collectors.toList());

		if (notMatchAll.isEmpty()) {
			return IAdhocFilter.MATCH_ALL;
		} else if (notMatchAll.size() == 1) {
			return notMatchAll.get(0);
		} else {
			return AndFilter.builder().filters(notMatchAll).build();
		}
	}

}