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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
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
@Slf4j
@Builder
public final class UnsafeMeasureForest implements IMeasureForest {
	@Getter
	@Default
	@SuppressWarnings("PMD.UnusedAssignment")
	final String name = "unsafe";

	@NonNull
	@Singular
	final Map<String, IMeasure> namedMeasures;

	// https://github.com/projectlombok/lombok/issues/1460#issuecomment-864253097
	private UnsafeMeasureForest(String name, Map<String, IMeasure> namedMeasures) {
		this.name = name;
		this.namedMeasures = new ConcurrentHashMap<>(namedMeasures);
	}

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return namedMeasures;
	}

	public void clear() {
		namedMeasures.clear();
	}

	/**
	 * @param measure
	 * @return this
	 */
	public UnsafeMeasureForest addMeasure(IMeasure measure) {
		String measureName = measure.getName();

		if (measureName == null) {
			throw new IllegalArgumentException("m=%s has a null name".formatted(measure));
		}

		namedMeasures.put(measureName, measure);

		return this;
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

	public static UnsafeMeasureForest fromMeasures(String name, List<IMeasure> measures) {
		UnsafeMeasureForest ams = UnsafeMeasureForest.builder().name(name).build();

		measures.forEach(measure -> {
			String measureName = measure.getName();
			if (ams.getNameToMeasure().containsKey(measureName)) {
				throw new IllegalArgumentException(
						"forest=%s Can not replace a measure in `.addMeasure`, Conflicting name is %s".formatted(name,
								measureName));
			}

			ams.addMeasure(measure);
		});

		return ams;
	}

	/**
	 * In {@link UnsafeMeasureForest}, a visitor both mutate current {@link IMeasureForest} and return the
	 * immutable+edited forest.
	 */
	@Override
	public IMeasureForest acceptVisitor(IMeasureForestVisitor visitor) {
		Set<IMeasure> measures = new LinkedHashSet<>();

		measures.addAll(visitor.addMeasures());
		getMeasures().forEach(m -> measures.addAll(visitor.mapMeasure(m)));

		this.namedMeasures.clear();
		measures.forEach(m -> this.namedMeasures.put(m.getName(), m));

		return this;
	}

	/**
	 * 
	 * @return a safe/immutable IMeasureForest based on current state of this {@link UnsafeMeasureForest}
	 */
	public IMeasureForest build() {
		return MeasureForest.fromMeasures(getName(), getMeasures());
	}

	/**
	 * Lombok @Builder
	 * 
	 * @author Benoit Lacelle
	 */
	public static class UnsafeMeasureForestBuilder {
		public UnsafeMeasureForestBuilder measure(IMeasure measure) {
			this.namedMeasure(measure.getName(), measure);

			return this;
		}
	}

}
