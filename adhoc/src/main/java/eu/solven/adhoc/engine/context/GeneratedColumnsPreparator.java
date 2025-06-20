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
package eu.solven.adhoc.engine.context;

import java.util.Set;

import eu.solven.adhoc.engine.ICanResolveMeasure;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * This {@link IQueryPreparator} helps always keeping given measures, generating given columns. It helps
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
@Deprecated(since = "Poor designs. Some IColumnGenerator should not come from measures")
public class GeneratedColumnsPreparator extends StandardQueryPreparator {

	@Singular
	Set<String> generatedColumnsMeasures;

	@Override
	protected MeasureForestBuilder filterForest(ICanResolveMeasure forest, ICubeQuery preparedQuery) {
		MeasureForestBuilder filteredForest = super.filterForest(forest, preparedQuery);

		generatedColumnsMeasures.forEach(calculatedMeasure -> filteredForest
				.measure(forest.resolveIfRef(ReferencedMeasure.ref(calculatedMeasure))));

		return filteredForest;
	}

}
