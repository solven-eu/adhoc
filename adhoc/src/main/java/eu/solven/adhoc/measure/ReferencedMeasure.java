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

import java.util.Comparator;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;

import eu.solven.adhoc.measure.model.IMeasure;
import eu.solven.adhoc.query.filter.value.IHasWrapped;
import eu.solven.adhoc.resource.HasWrappedSerializer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

/**
 * This is useful to refer to an existing {@link IMeasure} in the {@link MeasureForest}, hence preventing to need to
 * provide its definition.
 * 
 * @author Benoit Lacelle
 *
 */
@Value
@Builder
@Jacksonized
@JsonSerialize(using = HasWrappedSerializer.class)
@Slf4j
public class ReferencedMeasure implements IMeasure, IReferencedMeasure, IHasWrapped, Comparable<ReferencedMeasure> {
	// https://github.com/FasterXML/jackson-databind/issues/5030
	// @JsonValue
	@NonNull
	String ref;

	/**
	 * The name is the same as the ref, so a measure and its reference would conflict.
	 */
	@Override
	@JsonIgnore
	public String getName() {
		return ref;
	}

	@Override
	public Object getWrapped() {
		return getName();
	}

	@Override
	@JsonIgnore
	public Set<String> getTags() {
		return Set.of("reference");
	}

	@Override
	public IMeasure withTags(ImmutableSet<String> tags) {
		log.warn("Can not edit tags of {}", this);
		return this;
	}

	@Override
	public int compareTo(ReferencedMeasure o) {
		if (this.getClass() != o.getClass()) {
			return this.getClass().getName().compareTo(o.getClass().getName());
		}

		return Comparator.comparing(ReferencedMeasure::getRef).compare(this, o);
	}

	public static ReferencedMeasure ref(String name) {
		return ReferencedMeasure.builder().ref(name).build();
	}

	public static class ReferencedMeasureBuilder {

		public ReferencedMeasureBuilder() {
		}

		// Enable Jackson deserialization given a plain String
		public ReferencedMeasureBuilder(String measure) {
			this.ref(measure);
		}
	}
}
