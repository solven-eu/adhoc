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
package eu.solven.adhoc.table.transcoder;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;

import eu.solven.adhoc.column.FunctionCalculatedColumn;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Given an {@link ITableAliaser}, this will remember how each column has been aliased. It serves multiple purposes. The
 * first one is not to require {@link ITableAliaser} to express a bi-directional mapping. The second main purpose is the
 * reverse mapping may be ambiguous, when multiple queries columns could be served by the same underlying column.
 * 
 * @author Benoit Lacelle
 */
@Builder
@Slf4j
public class AliasingContext implements ITableAliaser, ITableReverseAliaser {
	// Most column would be managed through identity aliasing
	final Set<String> identity = new LinkedHashSet<>();
	// Some columns would be managed through not-trivial aliasing
	final SetMultimap<String, String> underlyingToQueried =
			MultimapBuilder.linkedHashKeys().linkedHashSetValues().build();

	@Getter
	final Map<String, FunctionCalculatedColumn> nameToCalculated = new LinkedHashMap<>();

	@Builder.Default
	final ITableAliaser transcoder = new IdentityImplicitAliaser();

	// Optimization performance
	private final Map<Set<String>, Long> cacheKeysToSize = new ConcurrentHashMap<>();

	@Override
	public String underlying(String queried) {
		String underlyingColumn = transcoder.underlyingNonNull(queried);

		if (Objects.equals(underlyingColumn, queried)) {
			identity.add(underlyingColumn);
		} else if (underlyingToQueried.put(underlyingColumn, queried)) {
			log.trace("Registered {} -> {} in {}", underlyingColumn, queried, this);
		}

		return underlyingColumn;
	}

	@Override
	public Set<String> queried(String underlying) {
		if (underlyingToQueried.containsKey(underlying)) {
			if (identity.contains(underlying)) {
				return ImmutableSet.<String>builder()
						.addAll(underlyingToQueried.get(underlying))
						.add(underlying)
						.build();
			} else {
				return underlyingToQueried.get(underlying);
			}
		} else if (identity.contains(underlying)) {
			return Set.of(underlying);
		} else if (nameToCalculated.containsKey(underlying)) {
			return Set.of(underlying);
		} else {
			return Set.of();
		}
	}

	public Set<String> underlyings() {
		if (underlyingToQueried.isEmpty()) {
			return identity;
		} else {
			// Merge the keys which are aliased and those which are not aliased
			return ImmutableSet.<String>builder().addAll(underlyingToQueried.keySet()).addAll(identity).build();
		}
	}

	public void addCalculatedColumn(FunctionCalculatedColumn calculatedColumn) {
		nameToCalculated.put(calculatedColumn.getName(), calculatedColumn);
	}

	@Override
	public int estimateQueriedSize(Set<String> underlyingKeys) {
		Long sizeAsLong = cacheKeysToSize.computeIfAbsent(underlyingKeys,
				keys -> keys.stream().mapToLong(k -> queried(k).size()).sum());
		return Ints.checkedCast(sizeAsLong);
	}

	@Override
	public String toString() {
		return underlyings().stream().map(u -> u + "->" + queried(u)).collect(Collectors.joining(", "));
	}

	public boolean isOnlyIdentity() {
		return underlyingToQueried.isEmpty();
	}
}
