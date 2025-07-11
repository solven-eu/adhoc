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
package eu.solven.adhoc.measure;

import java.util.Optional;
import java.util.Set;

import eu.solven.adhoc.engine.ICanResolveMeasure;
import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.table.ITableWrapper;
import eu.solven.adhoc.util.AdhocMapPathGet;
import eu.solven.adhoc.util.IHasName;

/**
 * Holds a {@link Set} of {@link IMeasure}, independent of an underlying {@link ITableWrapper}.
 * 
 * @author Benoit Lacelle
 */
public interface IMeasureForest extends IHasName, IHasMeasures, ICanResolveMeasure {

	/**
	 * Translate if necessary a {@link ReferencedMeasure} into a plain {@link IMeasure}
	 * 
	 * @param measure
	 * @return an actual {@link IMeasure}, never a {@link ReferencedMeasure}
	 */
	@Override
	default IMeasure resolveIfRef(IMeasure measure) {
		Optional<IMeasure> optResolved = resolveIfRefOpt(measure);

		return optResolved.orElseThrow(() -> {
			String refName = measure.getName();

			String minimizing = AdhocMapPathGet.minimizingDistance(getNameToMeasure().keySet(), refName);

			throw new IllegalArgumentException(
					"forest=%s No measure named: %s. Did you mean: %s".formatted(getName(), refName, minimizing));
		});
	}

	/**
	 * 
	 * @param measure
	 *            a measure, possibly a {@link ReferencedMeasure}.
	 * @return the optional plain {@link IMeasure}
	 */
	Optional<IMeasure> resolveIfRefOpt(IMeasure measure);

	IMeasureForest acceptVisitor(IMeasureForestVisitor visitor);

}
