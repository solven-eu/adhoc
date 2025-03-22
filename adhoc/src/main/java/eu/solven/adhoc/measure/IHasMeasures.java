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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.table.IAdhocTableWrapper;

/**
 * Holds a {@link Set} of {@link IMeasure}, independent of an underlying {@link IAdhocTableWrapper}.
 * 
 * @author Benoit Lacelle
 */
public interface IHasMeasures {

	/**
	 * This recalls such objects requires not to have {@link IMeasure} with conflicting names.
	 * 
	 * @return a {@link Map} from measure name to the measure.
	 */
	@JsonIgnore
	default Map<String, IMeasure> getNameToMeasure() {
		return getMeasures().stream().collect(Collectors.toUnmodifiableMap(m -> m.getName(), m -> m));
	}

	/**
	 * 
	 * @return the {@link Set} of {@link IMeasure}
	 */
	default Set<IMeasure> getMeasures() {
		return getNameToMeasure().values().stream().collect(Collectors.toSet());
	}

}
