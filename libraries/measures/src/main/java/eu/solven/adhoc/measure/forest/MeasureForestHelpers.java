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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

import eu.solven.adhoc.measure.IReferencedMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.util.map.AdhocMapPathGet;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Helpers around {@link IMeasureForest} / {@link MeasureForest}.
 *
 * @author Benoit Lacelle
 */
@UtilityClass
@Slf4j
public class MeasureForestHelpers {

	/**
	 * Extracts a sub-forest containing only the named measures and their (recursive) underlying measures.
	 *
	 * <p>
	 * Walks the dependency graph from the given roots, transitively collecting every measure reachable via
	 * {@link IHasUnderlyingMeasures#getUnderlyingNames()} and via {@link IReferencedMeasure#getRef()}. Diamond
	 * dependencies and cycles are handled — each measure is included at most once.
	 *
	 * <p>
	 * The result is named {@code source.getName() + "-sub"}. Use
	 * {@link #subForestOf(IMeasureForest, String, Collection)} to provide an explicit name.
	 *
	 * @param source
	 *            the source forest to filter
	 * @param rootMeasureNames
	 *            the names of the measures to keep (along with their transitive dependencies); each MUST resolve
	 *            against {@code source.getNameToMeasure()} or an {@link IllegalArgumentException} is thrown
	 * @return a new {@link MeasureForest} containing exactly the closure of the requested roots
	 * @throws IllegalArgumentException
	 *             if any root name is unknown to {@code source}
	 */
	public static IMeasureForest subForestOf(IMeasureForest source, Collection<String> rootMeasureNames) {
		return subForestOf(source, source.getName() + "-sub", rootMeasureNames);
	}

	/**
	 * Extracts a sub-forest containing only the named measures and their (recursive) underlying measures, naming the
	 * result {@code newName}.
	 *
	 * @param source
	 *            the source forest to filter
	 * @param newName
	 *            the name of the returned forest
	 * @param rootMeasureNames
	 *            the names of the measures to keep (along with their transitive dependencies); each MUST resolve
	 *            against {@code source.getNameToMeasure()} or an {@link IllegalArgumentException} is thrown
	 * @return a new {@link MeasureForest} containing exactly the closure of the requested roots
	 * @throws IllegalArgumentException
	 *             if any root name is unknown to {@code source}
	 */
	public static IMeasureForest subForestOf(IMeasureForest source,
			String newName,
			Collection<String> rootMeasureNames) {
		Map<String, IMeasure> nameToMeasure = source.getNameToMeasure();

		// Validate root names eagerly so the caller gets a clear error before any traversal. Mirrors the
		// "did you mean" hint that IMeasureForest#resolveIfRef produces.
		for (String rootName : rootMeasureNames) {
			if (!nameToMeasure.containsKey(rootName)) {
				String hint = AdhocMapPathGet.minimizingDistance(nameToMeasure.keySet(), rootName);
				throw new IllegalArgumentException(
						"forest=%s No measure named: %s. Did you mean: %s".formatted(source.getName(), rootName, hint));
			}
		}

		// BFS from the validated roots, transitively collecting every reachable measure. LinkedHashMap preserves
		// the insertion order so the result is stable (roots first, then their underlyings in BFS order).
		Map<String, IMeasure> kept = new LinkedHashMap<>();
		Deque<String> toVisit = new ArrayDeque<>(rootMeasureNames);

		while (!toVisit.isEmpty()) {
			String name = toVisit.poll();
			if (kept.containsKey(name)) {
				// Already visited — diamond / cycle protection.
				continue;
			}
			IMeasure measure = nameToMeasure.get(name);
			if (measure == null) {
				// The source forest may legitimately have unfinished chains (per IMeasureForest Javadoc) — a
				// transitive underlying may name a measure that isn't in the source. Drop it with a warning
				// rather than failing; the caller already validated the root names above.
				log.warn("forest={} sub-forest skipping unresolved underlying name={}", source.getName(), name);
				continue;
			}
			kept.put(name, measure);

			if (measure instanceof IReferencedMeasure ref) {
				toVisit.add(ref.getRef());
			}
			if (measure instanceof IHasUnderlyingMeasures hasUnderlying) {
				toVisit.addAll(hasUnderlying.getUnderlyingNames());
			}
		}

		return MeasureForest.builder().name(newName).measures(kept.values()).build();
	}
}
