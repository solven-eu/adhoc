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
package eu.solven.adhoc.resource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import eu.solven.adhoc.measure.IMeasureForest;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * A {@link List} of {@link IMeasureForest}, ensuring each forest to have a unique nam.
 * 
 * @author Benoit Lacelle
 */
@Value
@Builder
public class MeasureForests {
	@Singular
	final ImmutableList<IMeasureForest> forests;

	public int size() {
		return forests.size();
	}

	public IMeasureForest getForest(String name) {
		return getNameToForest().get(name);
	}

	public Set<String> forestNames() {
		return getNameToForest().keySet();
	}

	public Map<String, IMeasureForest> getNameToForest() {
		return forests.stream().collect(Collectors.toMap(IMeasureForest::getName, f -> f));
	}
}
