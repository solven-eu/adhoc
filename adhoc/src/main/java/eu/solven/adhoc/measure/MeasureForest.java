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
package eu.solven.adhoc.measure;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * The set of named {@link IMeasure}. Many of them are {@link IHasUnderlyingMeasures}, which express a
 * Directed-Acyclic-Graph. The DAG is actually evaluated on a per-query basis.
 *
 * This may not generate a single-connected DAG, as it may contains unfinished chains of measures.
 *
 * @author Benoit Lacelle
 *
 */
@RequiredArgsConstructor
@Slf4j
@Builder
@ToString(exclude = "cachedNameToMeasure")
public class MeasureForest implements IMeasureForest {
	@Getter
	@NonNull
	final String name;

	@NonNull
	@Singular
	final ImmutableList<IMeasure> measures;

	final Supplier<Map<String, IMeasure>> cachedNameToMeasure = Suppliers.memoize(this::noCacheNameToMeasures);

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return cachedNameToMeasure.get();
	}

	protected Map<String, IMeasure> noCacheNameToMeasures() {
		Map<String, IMeasure> nameToMeasure = new TreeMap<>();

		measures.forEach(m -> nameToMeasure.put(m.getName(), m));

		return nameToMeasure;
	}

	@Override
	public Optional<IMeasure> resolveIfRefOpt(IMeasure measure) {
		if (measure instanceof IReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = getNameToMeasure().get(refName);

			return Optional.ofNullable(resolved);
		}
		return Optional.of(measure);
	}

	public static MeasureForest fromMeasures(String name, Collection<? extends IMeasure> measures) {
		Map<String, IMeasure> nameToMeasure = new HashMap<>();

		measures.forEach(measure -> {
			String measureName = measure.getName();
			if (nameToMeasure.containsKey(measureName)) {
				throw new IllegalArgumentException(
						"forest=%s Can not replace a measure in `.addMeasure`, Conflicting name is %s".formatted(name,
								measureName));
			}

			nameToMeasure.put(measure.getName(), measure);
		});

		MeasureForestBuilder ams = MeasureForest.builder().name(name);

		ams.measures(nameToMeasure.values());

		return ams.build();
	}

	@Override
	public IMeasureForest acceptVisitor(IMeasureForestVisitor visitor) {
		Set<IMeasure> measures = new LinkedHashSet<>();

		measures.addAll(visitor.addMeasures());
		getMeasures().forEach(m -> measures.addAll(visitor.mapMeasure(m)));

		return MeasureForest.builder().name(getName()).measures(measures).build();
	}

	public static MeasureForestBuilder edit(IMeasureForest measures) {
		return MeasureForest.builder().name(measures.getName()).measures(measures.getNameToMeasure().values());
	}

	public static IMeasureForest empty() {
		return MeasureForest.builder().name("empty").build();
	}

}
