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
package eu.solven.adhoc.table.duckdb.var;

import java.util.Set;

import eu.solven.adhoc.engine.ICanResolveMeasure;
import eu.solven.adhoc.engine.context.DefaultQueryPreparator;
import eu.solven.adhoc.measure.IMeasureForest;
import eu.solven.adhoc.measure.MeasureForest.MeasureForestBuilder;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.query.cube.ICubeQuery;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * In this VaR cube, we needs some measures to be always available in the {@link IMeasureForest}. This is because we
 * want these columns to be working with unrelated measures like `COUNT(*)`.
 * 
 * @author Benoit Lacelle
 */
@SuperBuilder
public class GeneratedColumnsPreparator extends DefaultQueryPreparator {

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
