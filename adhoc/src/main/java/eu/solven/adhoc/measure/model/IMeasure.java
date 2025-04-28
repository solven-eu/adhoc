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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import eu.solven.adhoc.column.ReferencedColumn;
import eu.solven.adhoc.measure.MeasureForest;
import eu.solven.adhoc.measure.ReferencedMeasure;
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
@JsonSubTypes({ @JsonSubTypes.Type(value = ReferencedColumn.class, name = "ref"), })
public interface IMeasure extends IHasName, IHasTags {

	/**
	 * 
	 * @return the name of the {@link IMeasure}. It has to be unique within a given {@link MeasureForest}.
	 */
	String getName();

}
