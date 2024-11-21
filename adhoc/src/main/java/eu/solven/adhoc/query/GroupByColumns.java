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
package eu.solven.adhoc.query;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.Lists;

import eu.solven.adhoc.api.v1.IAdhocGroupBy;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GroupByColumns implements IAdhocGroupBy {
	NavigableSet<String> groupBy;

	@Override
	public NavigableSet<String> getGroupedByColumns() {
		return groupBy;
	}

	@Override
	public String toString() {
		if (isGrandTotal()) {
			return "grandTotal";
		}

		ToStringHelper toStringHelper = MoreObjects.toStringHelper(this).add("size", groupBy.size());

		AtomicInteger index = new AtomicInteger();
		groupBy.stream().limit(5).forEach(filter -> {
			toStringHelper.add("#" + index.getAndIncrement(), filter);
		});

		return toStringHelper.toString();
	}

	public static IAdhocGroupBy of(Collection<String> wildcards) {
		return GroupByColumns.builder().groupBy(new TreeSet<>(wildcards)).build();
	}

	public static IAdhocGroupBy of(String wildcard, String... wildcards) {
		return of(Lists.asList(wildcard, wildcards));
	}

}
