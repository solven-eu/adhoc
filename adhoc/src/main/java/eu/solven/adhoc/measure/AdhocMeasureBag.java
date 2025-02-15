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

import eu.solven.adhoc.measure.step.IHasUnderlyingMeasures;
import eu.solven.adhoc.resource.MeasuresSetFromResource;
import lombok.Builder;
import lombok.Builder.Default;
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
public class AdhocMeasureBag implements IAdhocMeasureBag {
	@Getter
	@NonNull
	final String name;

	@Default
	@Getter
	@NonNull
	final Map<String, IMeasure> nameToMeasure = new ConcurrentHashMap<>();

	/**
	 * @param measure
	 * @return this
	 */
	public AdhocMeasureBag addMeasure(IMeasure measure) {
		String measureName = measure.getName();

		nameToMeasure.put(measureName, measure);

		return this;
	}

	@Override
	public IMeasure resolveIfRef(IMeasure measure) {
		if (measure instanceof ReferencedMeasure ref) {
			String refName = ref.getRef();
			IMeasure resolved = nameToMeasure.get(refName);

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
			IMeasure resolved = nameToMeasure.get(refName);

			return Optional.ofNullable(resolved);
		}
		return Optional.of(measure);
	}

	public static AdhocMeasureBag fromMeasures(String name, List<IMeasure> measures) {
		AdhocMeasureBag ams = AdhocMeasureBag.builder().name(name).build();

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

	public AdhocMeasureBag acceptVisitor(IMeasureBagVisitor asCombinator) {
		return asCombinator.addMeasures(this);
	}

	// TODO Why doesn't this compile?
	// public static class AdhocMeasuresSetBuilder {
	// public AdhocMeasuresSetBuilder measure(IMeasure measure) {
	// this.nameToMeasure.put(measure.getName(), measure);
	//
	// return this;
	// }
	// }

}
