/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
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
package eu.solven.adhoc.column;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.springframework.util.ClassUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.solven.adhoc.collection.AdhocCollectionHelpers;
import eu.solven.adhoc.measure.model.IHasTags;
import eu.solven.adhoc.util.IHasName;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Holds static details about a column.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder(toBuilder = true)
public class ColumnMetadata implements IHasName, IHasTags {

	@NonNull
	String name;

	@Singular
	@NonNull
	ImmutableSet<String> tags;

	@NonNull
	@Default
	Class<?> type = Object.class;

	// Alternative names referring to this given (e.g. in SQL, a column may be qualified or not). The list may not be
	// exhaustive.
	@Singular
	ImmutableSet<String> aliases;

	/**
	 * 
	 * Typically useful in {@code CompositeCubesTableWrapper}, where a column can be fed by different underlying
	 * columns.
	 * 
	 * @param columns
	 *            a Set of {@link ColumnMetadata}. Must not be empty
	 * @return a ColumnMetadata representing the union of input columns.
	 */
	public static ColumnMetadata merge(Collection<? extends ColumnMetadata> columns) {
		if (columns.isEmpty()) {
			throw new IllegalArgumentException("Need at least one column");
		}

		// https://stackoverflow.com/questions/9797212/finding-the-nearest-common-superclass-or-superinterface-of-a-collection-of-cla
		// Spring's `determineCommonAncestor` returns `null` when the only shared ancestor is `Object` (it explicitly
		// filters Object out as too trivial — see ClassUtils.determineCommonAncestor). Streaming that operator into
		// `reduce` would crash with NPE the moment the accumulator becomes null (Stream.reduce wraps results in
		// Optional.of, which forbids null). Wrap the operator so a null result is normalised to `Object.class` —
		// callers depend on `merge` returning a non-null type, and Object is the correct upper bound for unrelated
		// classes.
		Optional<? extends Class<?>> commonType = columns.stream()
				.<Class<?>>map(ColumnMetadata::getType)
				.reduce((c1, c2) -> Objects.requireNonNullElse(ClassUtils.determineCommonAncestor(c1, c2),
						Object.class));

		// Keep as alias only if all definition holds given alias
		// This is better for composite cubes, as an alias valid for only a subCube should not be consider an alias in
		// the composite.
		// `reduce` MUST seed with the first element's aliases (NOT `ImmutableSet.of()`): seeding with empty makes
		// `Sets.intersection(empty, anything) == empty` short-circuit on the first iteration, so the result was
		// always empty regardless of input.
		Set<String> intersectionAliases = columns.stream()
				.<Set<String>>map(ColumnMetadata::getAliases)
				.reduce(Sets::intersection)
				.orElse(ImmutableSet.of());

		// In a composite cube, it seems legitimate to consider the union of tags. This would not be true of composite
		// tags like `composite-full`.
		Set<String> unionTags =
				columns.stream().<Set<String>>map(ColumnMetadata::getTags).reduce(ImmutableSet.of(), Sets::union);

		ColumnMetadata first = AdhocCollectionHelpers.getFirst(columns);

		return first.toBuilder()
				.type(commonType.get())
				.clearAliases()
				.aliases(intersectionAliases)
				.clearTags()
				.tags(unionTags)
				.build();
	}

}
