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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import eu.solven.adhoc.measure.AdhocMeasureBag;

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
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "type")
public interface IMeasure {

	/**
	 * 
	 * @return the name of the {@link IMeasure}. It has to be unique within a given {@link AdhocMeasureBag}.
	 */
	String getName();

	/**
	 * Tags are useful for various operations, like documentation (e.g. coloring some graphviz by tag).
	 * 
	 * @return the tags applied to this measure.
	 */
	Set<String> getTags();

	// JsonIgnore as implied by tags
	@JsonIgnore
	default boolean isDebug() {
		return getTags().contains("debug");
	}

}
