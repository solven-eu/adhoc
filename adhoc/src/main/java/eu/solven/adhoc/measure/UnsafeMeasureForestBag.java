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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.measure.transformator.IHasUnderlyingMeasures;
import eu.solven.adhoc.resource.MeasuresSetFromResource;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
public class UnsafeMeasureForestBag implements IMeasureForest {
	@Getter
	@NonNull
	final String name;

	@NonNull
	final Map<String, IMeasure> nameToMeasure = new ConcurrentHashMap<>();

	@Override
	public Map<String, IMeasure> getNameToMeasure() {
		return nameToMeasure;
	}

	/**
	 * @param measure
	 * @return this
	 */
	public UnsafeMeasureForestBag addMeasure(IMeasure measure) {
		String measureName = measure.getName();

		if (measureName == null) {
			throw new IllegalArgumentException("m=%s has a null name".formatted(measure));
		}

		nameToMeasure.put(measureName, measure);

		return this;
	}

	@Override
	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = getNameToMeasure().get(refName);

			if (resolved == null) {
				String minimizing = MeasuresSetFromResource.minimizingDistance(getNameToMeasure().keySet(), refName);

				throw new IllegalArgumentException(
						"bag=%s No measure named: %s. Did you mean: %s".formatted(name, refName, minimizing));
			}

			return resolved;
		}
		return measure;
	}

	@Override
	public Optional<IMeasure> resolveIfRefOpt(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = getNameToMeasure().get(refName);

			return Optional.ofNullable(resolved);
		}
		return Optional.of(measure);
	}

	public static UnsafeMeasureForestBag fromMeasures(String name, List<IMeasure> measures) {
		UnsafeMeasureForestBag ams = UnsafeMeasureForestBag.builder().name(name).build();

		measures.forEach(measure -> {
			String measureName = measure.getName();
			if (ams.getNameToMeasure().containsKey(measureName)) {
				throw new IllegalArgumentException(
						"bag=%s Can not replace a measure in `.addMeasure`, Conflicting name is %s".formatted(name,
								measureName));
			}

			ams.addMeasure(measure);
		});

		return ams;
	}

	/**
	 * In {@link UnsafeMeasureForestBag}, a visitor both mutate current {@link IMeasureForest} and return the
	 * immutable+edited bag.
	 */
	public IMeasureForest acceptVisitor(IMeasureBagVisitor asCombinator) {
		IMeasureForest newState = asCombinator.addMeasures(this);

		this.nameToMeasure.clear();
		this.nameToMeasure.putAll(newState.getNameToMeasure());

		return newState;
	}

	// public static AdhocMeasureBagBuilder edit(IAdhocMeasureBag measures) {
	// return UnsafeAdhocMeasureBag.builder().name(measures.getName()).measures(measures.getNameToMeasure().values());
	// }

	// TODO Why doesn't this compile?
	// public static class AdhocMeasuresSetBuilder {
	// public AdhocMeasuresSetBuilder measure(IMeasure measure) {
	// this.nameToMeasure.put(measure.getName(), measure);
	//
	// return this;
	// }
	// }

}
