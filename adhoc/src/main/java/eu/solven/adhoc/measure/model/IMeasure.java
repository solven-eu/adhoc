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
package eu.solven.adhoc.measure.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;

import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
import eu.solven.adhoc.measure.combination.FindFirstCombination;
import eu.solven.adhoc.util.IHasName;

/**
 * A node in a DAG of measures. Typically an {@link Aggregator} or a {@link Combinator}.
 * 
 * Any {@link IMeasure} should be (de)serializable with Jackson.
 * 
 * @author Benoit Lacelle
 *
 */
// https://dax.guide/st/measure/
// `@JsonTypeInfo` is ambiguous given MeasureSetFromResources. But it is useful for SchemaMetadata
// https://github.com/FasterXML/jackson-annotations/issues/279
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "type", defaultImpl = ReferencedMeasure.class)
@JsonSubTypes({ @JsonSubTypes.Type(value = ReferencedMeasure.class, name = "ref"), })
public interface IMeasure extends IHasName, IHasTags {

	/**
	 * 
	 * @return the name of the {@link IMeasure}. It has to be unique within a given {@link MeasureForest}.
	 */
	String getName();

	/**
	 * 
	 * @param name
	 * @param underlying
	 * @param moreUnderlyings
	 * @return an IMeasure doing the SUM of underlyings.
	 */
	static IMeasure sum(String name, String underlying, String... moreUnderlyings) {
		if (moreUnderlyings.length == 0) {
			return alias(name, underlying);
		} else {
			return Combinator.builder().name(name).underlyings(Lists.asList(underlying, moreUnderlyings)).build();
		}
	}

	/**
	 * 
	 * @param alias
	 *            the name of the name, i.e. the name with which one can refer to this alias.
	 * @param aliased
	 *            the name of the aliased measure, i.e. the name of the underlying/actually defined measure
	 * @return
	 */
	static IMeasure alias(String alias, String aliased) {
		if (Objects.equals(alias, aliased)) {
			return ReferencedMeasure.ref(alias);
		} else {
			return Combinator.builder()
					.name(alias)
					.combinationKey(FindFirstCombination.KEY)
					.underlying(aliased)
					.build();
		}
	}
}
