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
package eu.solven.adhoc.table.duckdb.var;

import java.util.Map;

import eu.solven.adhoc.data.cell.IValueProvider;
import eu.solven.adhoc.data.row.ISlicedRecord;
import eu.solven.adhoc.engine.step.ISliceWithStep;
import eu.solven.adhoc.measure.combination.ICombination;
import eu.solven.adhoc.query.filter.FilterHelpers;

/**
 * Enabling a mapping from the column specifying a scenarioIndex to a column specifying a scenarioName.
 * 
 * @author Benoit Lacelle
 */
public class ExampleVaRScenarioNameCombination implements ICombination {

	public static final String KEY = "ARRAY";

	protected final int nbScenarios;

	public ExampleVaRScenarioNameCombination(Map<String, ?> options) {
		Object rawNbScenarios = options.get("nbScenarios");
		nbScenarios = ((Number) rawNbScenarios).intValue();
	}

	@Override
	public IValueProvider combine(ISliceWithStep slice, ISlicedRecord slicedRecord) {
		if (!FilterHelpers.getFilteredColumns(slice.asFilter()).contains(IExampleVaRConstants.C_SCENARIOINDEX)) {
			return vc -> vc.onObject(noScenario());
		}

		Object filteredScenarioIndex =
				FilterHelpers.getValueMatcher(slice.asFilter(), IExampleVaRConstants.C_SCENARIOINDEX);

		if (filteredScenarioIndex instanceof Number filteredScenarioIndexAsNumber) {
			int indexAsInt = filteredScenarioIndexAsNumber.intValue();

			Object scenarioName = indexToName(indexAsInt);
			return vc -> vc.onObject(scenarioName);
		} else {
			// BEWARE Unclear case: some measure generated an unexpected scenarioIndex
			return vc -> vc.onObject(filteredScenarioIndex);
		}
	}

	public static String indexToName(int indexAsInt) {
		return "histo_" + indexAsInt;
	}

	/**
	 * May be null.
	 * 
	 * @return the value to return when no scenario has been selected.
	 */
	protected Object noScenario() {
		return "no_scenario";
	}

}
